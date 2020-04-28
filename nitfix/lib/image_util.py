"""Utilities for working with images.

Particularly feature extraction.
"""

from collections import namedtuple
from PIL import Image, ImageFilter
import zbarlight    # pylint: disable=import-error
import zbar
from .util import PHOTOS


Dimensions = namedtuple('Dimensions', 'width height')


def qr_value(image_file):
    """Read and process image."""
    image = open_image(image_file)
    return get_qr_code(image) if image else None


def open_image(image_file):
    """Open an image."""
    path = str(image_file)
    path = image_file if '/' in path else PHOTOS / image_file
    with open(path, 'rb') as image_fh:
        try:
            image = Image.open(image_fh)
            image.load()
        except OSError:
            return None
    return image


def get_qr_code(image):
    """
    Extract QR code from image.

    Try various methods to find the QR code in the image. Starting from
    quickest and moving to the most unlikely method.
    """
    qr_code = zbarlight.scan_codes('qrcode', image)
    if qr_code:
        return qr_code[0].decode('utf-8')

    qr_code = get_qr_code_using_slider(image)
    if qr_code:
        return qr_code

    qr_code = get_qr_code_by_rotation(image)
    if qr_code:
        return qr_code

    return get_qr_code_by_sharpening(image)


def get_qr_code_using_slider(image):
    """Try sliding a window over the image to search for the QR code."""
    for slider in window_slider(image):
        cropped = image.crop(slider)
        qr_code = zbarlight.scan_codes('qrcode', cropped)
        if qr_code:
            return qr_code[0].decode('utf-8')
    return None


def get_qr_code_by_rotation(image):
    """Try rotating the image to find the QR code *sigh*."""
    for degrees in range(5, 85, 5):
        rotated = image.rotate(degrees)
        qr_code = zbarlight.scan_codes('qrcode', rotated)
        if qr_code:
            return qr_code[0].decode('utf-8')
    return None


def get_qr_code_by_sharpening(image):
    """Try to sharpen the image to find the QR code."""
    sharpened = image.filter(ImageFilter.SHARPEN)
    qr_code = zbarlight.scan_codes('qrcode', sharpened)
    if qr_code:
        return qr_code[0].decode('utf-8')
    return None


def window_slider(image_size, window=None, stride=None):
    """
    Create slider window.

    It helps with feature extraction by limiting the search area.
    """
    window = window if window else Dimensions(400, 400)
    stride = stride if stride else Dimensions(200, 200)

    for top in range(0, image_size.height, stride.height):
        bottom = top + window.height
        bottom = image_size.height if bottom > image_size.height else bottom

        for left in range(0, image_size.width, stride.width):
            right = left + window.width
            right = image_size.width if right > image_size.width else right

            slider = (left, top, right, bottom)

            yield slider


def locate_qr_code(image):
    """Find the location of a QR code in the image."""
    scanner = zbar.Scanner()  # Memory leaks somewhere, so be careful here
    gray = image.convert('L')

    results = scanner.scan(gray)
    if results:
        return bbox(results)

    results = locate_qr_code_using_slider(gray, scanner)
    if results:
        return results

    return None


def locate_qr_code_using_slider(image, scanner):
    """Try sliding a window over the image to search for the QR code."""
    for slider in window_slider(image):
        cropped = image.crop(slider)
        results = scanner.scan(cropped)
        if results:
            box = bbox(results)
            # Box is relative to the window, we want the position in the image
            box = (
                box[0] + slider[0],
                box[1] + slider[1],
                box[2] + slider[0],
                box[3] + slider[1])
            return box
    return None


def bbox(results):
    """Return the bounding box of the QR code given the results."""
    corners = results[0].position if results else []
    return (
        min(c[0] for c in corners),
        min(c[1] for c in corners),
        max(c[0] for c in corners),
        max(c[1] for c in corners))
