# jobs/image_processing/transforms.py
# All image transform operations.
# Each function takes a PIL Image and returns a new PIL Image.
# Workers call these functions by name — the name is passed in the task payload.
#
# Supported transforms:
#   grayscale       — convert to single-channel grayscale
#   brightness      — increase / decrease brightness (factor param)
#   contrast        — increase / decrease contrast   (factor param)
#   blur            — Gaussian blur                  (radius param)
#   sharpen         — unsharp mask sharpening
#   resize          — resize to a given width×height (keeping aspect ratio)
#   thumbnail       — resize to fit inside a box (e.g. 128×128)
#   flip_horizontal — mirror left-right
#   flip_vertical   — mirror top-bottom
#   rotate          — rotate by degrees              (degrees param)
#   edge_detect     — find edges using FIND_EDGES filter
#   sepia           — warm sepia tone effect
#   invert          — colour inversion (negative)

from PIL import Image, ImageFilter, ImageEnhance, ImageOps
from typing import Any


# ── Registry: name → function ─────────────────────────────────────────────────

def grayscale(img: Image.Image, **_) -> Image.Image:
    """Convert to grayscale (L mode), then back to RGB for uniform output."""
    return img.convert("L").convert("RGB")


def brightness(img: Image.Image, factor: float = 1.5, **_) -> Image.Image:
    """
    Adjust brightness.
    factor > 1 brightens, factor < 1 darkens, factor = 1 is unchanged.
    """
    return ImageEnhance.Brightness(img).enhance(float(factor))


def contrast(img: Image.Image, factor: float = 1.5, **_) -> Image.Image:
    """
    Adjust contrast.
    factor > 1 increases contrast, < 1 reduces it.
    """
    return ImageEnhance.Contrast(img).enhance(float(factor))


def blur(img: Image.Image, radius: float = 2.0, **_) -> Image.Image:
    """Apply Gaussian blur with the given radius."""
    return img.filter(ImageFilter.GaussianBlur(radius=float(radius)))


def sharpen(img: Image.Image, **_) -> Image.Image:
    """Sharpen using unsharp mask."""
    return img.filter(ImageFilter.UnsharpMask(radius=2, percent=150, threshold=3))


def resize(img: Image.Image, width: int = 256, height: int = 256, **_) -> Image.Image:
    """
    Resize to exactly width×height pixels.
    Use thumbnail() if you want aspect-ratio-preserving resize.
    """
    return img.resize((int(width), int(height)), Image.LANCZOS)


def thumbnail(img: Image.Image, size: int = 128, **_) -> Image.Image:
    """
    Resize to fit inside a size×size box while preserving aspect ratio.
    Returns a new image (does not modify in-place).
    """
    out = img.copy()
    out.thumbnail((int(size), int(size)), Image.LANCZOS)
    return out


def flip_horizontal(img: Image.Image, **_) -> Image.Image:
    """Mirror the image left to right."""
    return ImageOps.mirror(img)


def flip_vertical(img: Image.Image, **_) -> Image.Image:
    """Flip the image top to bottom."""
    return ImageOps.flip(img)


def rotate(img: Image.Image, degrees: float = 90.0, **_) -> Image.Image:
    """
    Rotate counter-clockwise by degrees.
    expand=True grows the canvas to fit the rotated image.
    """
    return img.rotate(float(degrees), expand=True)


def edge_detect(img: Image.Image, **_) -> Image.Image:
    """
    Highlight edges using PIL's FIND_EDGES filter.
    Converts to grayscale first for cleaner results.
    """
    gray = img.convert("L")
    edges = gray.filter(ImageFilter.FIND_EDGES)
    return edges.convert("RGB")


def sepia(img: Image.Image, **_) -> Image.Image:
    """
    Apply a warm sepia tone by tinting a grayscale version.
    Uses a matrix transform for efficiency.
    """
    gray = img.convert("L").convert("RGB")
    r, g, b = gray.split()

    def _apply(channel, rf, gf, bf):
        # blend using point transforms
        return channel

    # Sepia matrix applied via ImageEnhance chain
    sepia_img = ImageEnhance.Color(gray).enhance(0)     # desaturate
    sepia_img = ImageEnhance.Brightness(sepia_img).enhance(1.1)
    # Tint: manually apply sepia colour shift via pixel manipulation
    sepia_data = []
    for pixel in sepia_img.getdata():
        r_val = min(255, int(pixel[0] * 1.0))
        g_val = min(255, int(pixel[1] * 0.85))
        b_val = min(255, int(pixel[2] * 0.65))
        sepia_data.append((r_val, g_val, b_val))
    result = Image.new("RGB", sepia_img.size)
    result.putdata(sepia_data)
    return result


def invert(img: Image.Image, **_) -> Image.Image:
    """Invert all pixel values (create a negative)."""
    return ImageOps.invert(img.convert("RGB"))


# ── Transform registry ────────────────────────────────────────────────────────

TRANSFORMS = {
    "grayscale"       : grayscale,
    "brightness"      : brightness,
    "contrast"        : contrast,
    "blur"            : blur,
    "sharpen"         : sharpen,
    "resize"          : resize,
    "thumbnail"       : thumbnail,
    "flip_horizontal" : flip_horizontal,
    "flip_vertical"   : flip_vertical,
    "rotate"          : rotate,
    "edge_detect"     : edge_detect,
    "sepia"           : sepia,
    "invert"          : invert,
}


def apply_transform(img: Image.Image, transform_name: str,
                    params: dict = None) -> Image.Image:
    """
    Look up and apply a named transform.

    Args:
        img:            Input PIL Image.
        transform_name: Key from TRANSFORMS dict.
        params:         Optional dict of kwargs passed to the transform fn.

    Returns:
        Transformed PIL Image.

    Raises:
        ValueError if transform_name is not in TRANSFORMS.
    """
    fn = TRANSFORMS.get(transform_name)
    if fn is None:
        available = list(TRANSFORMS.keys())
        raise ValueError(
            f"Unknown transform: {transform_name!r}. "
            f"Available: {available}")
    return fn(img, **(params or {}))


def list_transforms() -> list:
    """Return a sorted list of all available transform names."""
    return sorted(TRANSFORMS.keys())
