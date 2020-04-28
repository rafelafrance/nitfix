#!/usr/bin/env python

"""Adjust Image Colors to remove differences in photographic conditions."""

import os
import multiprocessing
from random import shuffle

import matplotlib
from matplotlib import patches
import matplotlib.pyplot as plt
import numpy as np
from PIL import Image

import lib.db as db
import lib.util as util
import lib.image_util as i_util


OUT_DIR = util.RAW_DATA / 'adjusted'
EXEMPLAR = util.PHOTOS / 'Tingshuang_TEX_nitfix_photos' / 'L1040918.JPG'

PROCESSES = max(1, min(10, os.cpu_count() - 4))


def adjust_images():
    """Adjust image colors."""
    albums = get_albums()

    for album in albums:
        path = OUT_DIR / album[0]['album']
        path.mkdir(exist_ok=True)

    with multiprocessing.Pool(processes=PROCESSES) as pool:
        results = [pool.apply_async(adjust_album, (a, )) for a in albums]
        results = [r.get() for r in results]

    found = sum(r for r in results)
    total = sum(len(a) for a in albums)
    print(f'Adjusted {found} / {total}')


def adjust_album(album):
    """Adjust one album of images."""
    # Get exemplar image data
    image = get_image(EXEMPLAR)
    inner, outer = get_image_rectangles(image)
    target = get_average_envelope_colors(image, inner, outer)

    found = 0
    for photo in album:
        out_path = OUT_DIR / photo['album'] / photo['photo']
        if not out_path.exists():
            path = util.PHOTOS / photo['image_file']
            image = get_image(path)
            found += output_2_up(image, target, out_path)
    return found


def get_image(path):
    """Read in the image and rotate it if needed."""
    original = Image.open(path)
    transformed = original.resize((
        int(original.size[0] * 0.75),
        int(original.size[1] * 0.75)))

    dir_name = str(path.parent)
    if original.size[0] > original.size[1]:
        if (dir_name.startswith('Tingshuang')
            and dir_name != 'Tingshuang_US_nitfix_photos') \
                or dir_name in (
                'MO-DOE-nitfix_visit3', 'NY_DOE-nitfix_visit3',
                'NY_DOE-nitfix_visit4', 'NY_DOE-nitfix_visit5'):
            transformed = transformed.transpose(Image.ROTATE_90)
        else:
            transformed = transformed.transpose(Image.ROTATE_270)

    return transformed


def get_albums():
    """Get all images from the database."""
    step = 2000  # Basic set size
    photos = []

    with db.connect() as cxn:
        cursor = cxn.execute('select * from images')
        for image in cursor:
            path = util.PHOTOS / image[0]
            if path.exists():
                photos.append({'image_file': image[0], 'sample_id': image[1]})

    shuffle(photos)

    *albums, residual = [photos[i:i+step] for i in range(0, len(photos), step)]
    albums[-1] += residual

    for i, album in enumerate(albums, 1):
        out_dir = f'album_{i}'
        for photo in album:
            photo['album'] = out_dir
            photo['photo'] = photo['image_file'].replace('/', '_')

    return albums


def expand_bbox(bbox):
    """Create a bigger box surrounding the inner one."""
    pad = 75  # Pad the QR-Code bounding box by this many pixels
    return (
        min(bbox[0], bbox[2]) - pad,
        min(bbox[1], bbox[3]) - pad,
        max(bbox[0], bbox[2]) + pad,
        max(bbox[1], bbox[3]) + pad)


def draw_rectangle(ax, bbox, color):
    """A helper function for drawing boxes on images."""
    wide = abs(bbox[2] - bbox[0])
    high = abs(bbox[3] - bbox[1])
    rect = patches.Rectangle(
        bbox[:2], wide, high, fill=False, edgecolor=color, lw=2)
    ax.add_patch(rect)


def get_image_rectangles(image):
    """Get the inner and outer rectangles around the QR-code."""
    inner = i_util.locate_qr_code(image)
    if not inner:
        return None, None
    outer = expand_bbox(inner)
    return inner, outer


def get_average_envelope_colors(image, inner, outer):
    """Get the average RGB Values"""
    data = np.asarray(image, dtype='int64')

    crop = data[inner[1]:inner[3], inner[0]:inner[2]]
    inner_sum = crop.sum(axis=0).sum(axis=0)
    inner_size = crop.size

    crop = data[outer[1]:outer[3], outer[0]:outer[2]]
    outer_sum = crop.sum(axis=0).sum(axis=0)
    outer_size = crop.size

    average = (outer_sum - inner_sum) / (outer_size - inner_size)
    return average


def adjust_image(image, diff):
    """Adjust entire image based on the difference in envelope colors."""
    data = np.asarray(image, dtype='float32')

    data[:, :, 0] = np.clip(data[:, :, 0] - diff[0], 0.0, 255.0)
    data[:, :, 1] = np.clip(data[:, :, 1] - diff[1], 0.0, 255.0)
    data[:, :, 2] = np.clip(data[:, :, 2] - diff[2], 0.0, 255.0)

    return Image.fromarray(data.astype(np.uint8))


def output_2_up(image, target, out_path):
    """Write images to output file."""
    dpi = matplotlib.rcParams['figure.dpi']

    inner, outer = get_image_rectangles(image)

    width, height = image.size
    fig_size = width / float(dpi), height / float(dpi)

    fig, axes = plt.subplots(
        ncols=2, figsize=fig_size, constrained_layout=True)

    found = 1
    if inner:
        avg = get_average_envelope_colors(image, inner, outer)
        diff = avg - target
        draw_rectangle(axes[0], outer, 'blue')
        draw_rectangle(axes[0], inner, 'cyan')
        adjusted = adjust_image(image, diff)
    else:
        found = 0
        adjusted = image

    axes[0].imshow(image)
    axes[1].imshow(adjusted)

    out_path = str(out_path)
    plt.savefig(out_path)

    plt.close('all')  # Memory leak
    return found


if __name__ == '__main__':
    adjust_images()
