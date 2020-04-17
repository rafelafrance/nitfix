#!/usr/bin/env python

"""Adjust Image Colors to remove differences in photographic conditions."""

import matplotlib
from matplotlib import patches
import matplotlib.pyplot as plt
import numpy as np

from tqdm import tqdm
from PIL import Image

import lib.util as util
import lib.image_util as i_util


IN_DIR = util.SAMPLE_DATA / 'nitfix_sample_2400_2020-04-07a'
OUT_DIR = util.SAMPLE_DATA / 'nitfix_sample_2400_2020-04-07c_adjusted'
EXEMPLAR = str(IN_DIR / 'Tingshuang_TEX_nitfix_photos_L1040918.JPG')


def adjust_images():
    """Adjust Image Colors."""
    in_paths = list(IN_DIR.glob('*.[Jj][Pp][Gg]'))

    # Get the exemplar image data
    idx = get_image_index(in_paths, EXEMPLAR)
    image = get_image(in_paths, idx)
    inner, outer = get_image_rectangles(image)
    target = get_average_envelope_colors(image, inner, outer)

    for idx, path in tqdm(enumerate(in_paths)):
        name = path.name
        out_path = OUT_DIR / name

        if not out_path.exists():
            image = get_image(in_paths, idx)
            output_images(image, target, out_path)


def get_image(in_paths, idx):
    """Get the image from the path index."""
    path = str(in_paths[idx])
    return Image.open(path)


def get_image_index(in_paths, image_path):
    """get the index of the image from its name."""
    for i, path in enumerate(in_paths):
        path = str(path)
        if path == image_path:
            return i
    return None


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
        print('Could not find the QR-code.')
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


def output_images(image, target, out_path):
    """Write images to output file."""
    dpi = matplotlib.rcParams['figure.dpi']

    inner, outer = get_image_rectangles(image)

    width, height = image.size
    fig_size = width / float(dpi), height / float(dpi)

    fig, axes = plt.subplots(
        ncols=2, figsize=fig_size, constrained_layout=True)

    if inner:
        avg = get_average_envelope_colors(image, inner, outer)
        diff = avg - target
        draw_rectangle(axes[0], outer, 'blue')
        draw_rectangle(axes[0], inner, 'cyan')
        adjusted = adjust_image(image, diff)
    else:
        adjusted = image

    axes[0].imshow(image)
    axes[1].imshow(adjusted)

    out_path = str(out_path)
    plt.savefig(out_path)

    plt.close('all')  # Memory leak


if __name__ == '__main__':
    adjust_images()
