# tests/test_transforms.py
# Unit tests for every transform in jobs/image_processing/transforms.py
# Uses small synthetic PIL images — no files on disk needed.

import sys, os, unittest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from PIL import Image
from jobs.image_processing.transforms import (
    apply_transform, list_transforms, TRANSFORMS,
    grayscale, brightness, contrast, blur, sharpen,
    resize, thumbnail, flip_horizontal, flip_vertical,
    rotate, edge_detect, sepia, invert,
)


def _img(w=80, h=60, color=(120, 80, 200)):
    """Create a small solid-colour RGB test image."""
    return Image.new("RGB", (w, h), color)


def _gradient(w=80, h=60):
    """Create a gradient image for edge/filter tests."""
    img = Image.new("RGB", (w, h))
    for x in range(w):
        for y in range(h):
            img.putpixel((x, y), (int(255*x/w), int(255*y/h), 128))
    return img


class TestRegistry(unittest.TestCase):

    def test_list_transforms_returns_all(self):
        names = list_transforms()
        for name in ("grayscale","brightness","contrast","blur","sharpen",
                     "resize","thumbnail","flip_horizontal","flip_vertical",
                     "rotate","edge_detect","sepia","invert"):
            self.assertIn(name, names)

    def test_apply_transform_unknown_raises(self):
        with self.assertRaises(ValueError):
            apply_transform(_img(), "nonexistent_transform")

    def test_apply_transform_returns_pil_image(self):
        for name in list_transforms():
            result = apply_transform(_img(), name)
            self.assertIsInstance(result, Image.Image,
                                  f"{name} did not return a PIL Image")

    def test_all_transforms_return_rgb(self):
        for name in list_transforms():
            result = apply_transform(_img(), name)
            self.assertEqual(result.mode, "RGB",
                             f"{name} returned mode {result.mode}, expected RGB")


class TestGrayscale(unittest.TestCase):

    def test_returns_rgb_mode(self):
        self.assertEqual(grayscale(_img()).mode, "RGB")

    def test_desaturates_colours(self):
        img    = _img(color=(200, 50, 20))
        result = grayscale(img)
        # All pixels should have equal R, G, B after grayscale→RGB
        px = result.getpixel((0, 0))
        self.assertEqual(px[0], px[1])
        self.assertEqual(px[1], px[2])

    def test_same_size(self):
        img = _img(100, 80)
        self.assertEqual(grayscale(img).size, (100, 80))


class TestBrightness(unittest.TestCase):

    def test_brighten_increases_values(self):
        img    = _img(color=(100, 100, 100))
        result = brightness(img, factor=2.0)
        px = result.getpixel((0, 0))
        self.assertGreater(px[0], 100)

    def test_darken_decreases_values(self):
        img    = _img(color=(200, 200, 200))
        result = brightness(img, factor=0.5)
        px = result.getpixel((0, 0))
        self.assertLess(px[0], 200)

    def test_factor_one_unchanged(self):
        img    = _img(color=(150, 150, 150))
        result = brightness(img, factor=1.0)
        self.assertEqual(result.getpixel((0, 0)), img.getpixel((0, 0)))

    def test_same_size(self):
        self.assertEqual(brightness(_img(60, 40)).size, (60, 40))


class TestContrast(unittest.TestCase):

    def test_returns_image(self):
        self.assertIsInstance(contrast(_img()), Image.Image)

    def test_same_size(self):
        self.assertEqual(contrast(_img(60, 40)).size, (60, 40))

    def test_high_contrast_differs_from_low(self):
        img   = _gradient()
        high  = contrast(img, factor=3.0)
        low   = contrast(img, factor=0.3)
        self.assertNotEqual(list(high.getdata()), list(low.getdata()))


class TestBlur(unittest.TestCase):

    def test_returns_image(self):
        self.assertIsInstance(blur(_img()), Image.Image)

    def test_same_size(self):
        self.assertEqual(blur(_img(60, 40)).size, (60, 40))

    def test_blurred_differs_from_original(self):
        img    = _gradient()
        result = blur(img, radius=3.0)
        self.assertNotEqual(list(img.getdata()), list(result.getdata()))


class TestSharpen(unittest.TestCase):

    def test_returns_image(self):
        self.assertIsInstance(sharpen(_img()), Image.Image)

    def test_same_size(self):
        self.assertEqual(sharpen(_img(60, 40)).size, (60, 40))


class TestResize(unittest.TestCase):

    def test_exact_size(self):
        result = resize(_img(200, 150), width=64, height=64)
        self.assertEqual(result.size, (64, 64))

    def test_different_aspect(self):
        result = resize(_img(100, 100), width=200, height=50)
        self.assertEqual(result.size, (200, 50))

    def test_default_size(self):
        result = resize(_img(300, 300))
        self.assertEqual(result.size, (256, 256))


class TestThumbnail(unittest.TestCase):

    def test_fits_in_box(self):
        result = thumbnail(_img(200, 100), size=64)
        self.assertLessEqual(result.size[0], 64)
        self.assertLessEqual(result.size[1], 64)

    def test_aspect_ratio_preserved(self):
        # 200×100 → thumbnail 64 → should be 64×32
        result = thumbnail(_img(200, 100), size=64)
        w, h   = result.size
        self.assertAlmostEqual(w/h, 200/100, delta=1)

    def test_does_not_upscale(self):
        # small image stays small when box is larger
        img    = _img(32, 32)
        result = thumbnail(img, size=128)
        self.assertLessEqual(result.size[0], 128)


class TestFlips(unittest.TestCase):

    def test_horizontal_flip_same_size(self):
        self.assertEqual(flip_horizontal(_img(80, 60)).size, (80, 60))

    def test_vertical_flip_same_size(self):
        self.assertEqual(flip_vertical(_img(80, 60)).size, (80, 60))

    def test_horizontal_changes_pixels(self):
        img    = _gradient()
        result = flip_horizontal(img)
        # Leftmost column should now be different
        self.assertNotEqual(img.getpixel((0, 0)), result.getpixel((0, 0)))

    def test_double_flip_restores_original(self):
        img    = _gradient()
        result = flip_horizontal(flip_horizontal(img))
        self.assertEqual(list(img.getdata()), list(result.getdata()))


class TestRotate(unittest.TestCase):

    def test_returns_image(self):
        self.assertIsInstance(rotate(_img()), Image.Image)

    def test_180_same_size(self):
        # 180° rotation keeps dimensions
        result = rotate(_img(80, 60), degrees=180)
        self.assertEqual(result.size, (80, 60))

    def test_changes_pixels(self):
        img    = _gradient()
        result = rotate(img, degrees=90)
        self.assertNotEqual(list(img.getdata()), list(result.getdata()))


class TestEdgeDetect(unittest.TestCase):

    def test_returns_rgb(self):
        self.assertEqual(edge_detect(_img()).mode, "RGB")

    def test_same_size(self):
        self.assertEqual(edge_detect(_img(80, 60)).size, (80, 60))

    def test_solid_image_produces_mostly_black(self):
        img    = _img(color=(100, 150, 200))
        result = edge_detect(img)
        # Solid colour → no edges → most pixels close to black
        pixels = list(result.getdata())
        dark   = sum(1 for p in pixels if p[0] < 20)
        self.assertGreater(dark / len(pixels), 0.8)


class TestSepia(unittest.TestCase):

    def test_returns_rgb(self):
        self.assertEqual(sepia(_img()).mode, "RGB")

    def test_same_size(self):
        self.assertEqual(sepia(_img(80, 60)).size, (80, 60))

    def test_warm_tones(self):
        img    = _img(color=(128, 128, 128))
        result = sepia(img)
        px     = result.getpixel((0, 0))
        # Sepia: red channel should be >= green >= blue
        self.assertGreaterEqual(px[0], px[2])


class TestInvert(unittest.TestCase):

    def test_returns_rgb(self):
        self.assertEqual(invert(_img()).mode, "RGB")

    def test_inverts_values(self):
        img    = _img(color=(100, 150, 200))
        result = invert(img)
        px     = result.getpixel((0, 0))
        self.assertAlmostEqual(px[0], 255 - 100, delta=2)
        self.assertAlmostEqual(px[1], 255 - 150, delta=2)
        self.assertAlmostEqual(px[2], 255 - 200, delta=2)

    def test_double_invert_restores(self):
        img    = _gradient()
        result = invert(invert(img))
        orig   = list(img.getdata())
        res    = list(result.getdata())
        # Allow tiny rounding differences
        diffs  = [abs(o[i]-r[i]) for o,r in zip(orig,res) for i in range(3)]
        self.assertLess(max(diffs), 3)


if __name__ == "__main__": unittest.main()
