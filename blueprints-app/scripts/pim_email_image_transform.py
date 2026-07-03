"""Secret-free PIM Email image transform helper for remote workers."""

from __future__ import annotations

import hashlib
import os
from io import BytesIO

from PIL import Image, ImageOps, UnidentifiedImageError

TRANSFORM_VERSION = "jpeg-v1"
DEFAULT_REMOTE_IMAGE_MAX_BYTES = 25 * 1024 * 1024
MAX_IMAGE_PIXELS = 12_000_000
MAX_IMAGE_DIMENSIONS = (1800, 2400)

Image.MAX_IMAGE_PIXELS = MAX_IMAGE_PIXELS


class ImageTransformError(ValueError):
    pass


def remote_image_max_bytes() -> int:
    raw = os.environ.get("BLUEPRINTS_EMAIL_REMOTE_IMAGE_MAX_BYTES", "").strip()
    if not raw:
        return DEFAULT_REMOTE_IMAGE_MAX_BYTES
    try:
        parsed = int(raw)
    except ValueError:
        return DEFAULT_REMOTE_IMAGE_MAX_BYTES
    return max(1 * 1024 * 1024, parsed)


def transform_image_to_jpeg(content: bytes) -> bytes:
    if not content or len(content) > remote_image_max_bytes():
        raise ImageTransformError("image payload is empty or too large")
    try:
        with Image.open(BytesIO(content)) as opened:
            opened.seek(0)
            image = ImageOps.exif_transpose(opened)
            if image.mode in {"RGBA", "LA", "P"}:
                rgba = image.convert("RGBA")
                flattened = Image.new("RGB", rgba.size, (255, 255, 255))
                flattened.paste(rgba, mask=rgba.getchannel("A"))
                image = flattened
            elif image.mode != "RGB":
                image = image.convert("RGB")
            image.thumbnail(MAX_IMAGE_DIMENSIONS, Image.Resampling.LANCZOS)
            output = BytesIO()
            image.save(output, format="JPEG", quality=85, optimize=True, progressive=True)
            return output.getvalue()
    except (OSError, UnidentifiedImageError, ValueError, Image.DecompressionBombError) as exc:
        raise ImageTransformError("image could not be decoded safely") from exc


def jpeg_dimensions(content: bytes) -> tuple[int, int]:
    if not content or len(content) > remote_image_max_bytes():
        raise ImageTransformError("image payload is empty or too large")
    try:
        with Image.open(BytesIO(content)) as image:
            if image.format != "JPEG":
                raise ImageTransformError("transformed image is not a JPEG")
            width, height = image.size
            image.verify()
    except ImageTransformError:
        raise
    except (OSError, UnidentifiedImageError, ValueError, Image.DecompressionBombError) as exc:
        raise ImageTransformError("image could not be decoded safely") from exc
    if width <= 0 or height <= 0:
        raise ImageTransformError("transformed image dimensions are invalid")
    return int(width), int(height)


def sha256_hex(content: bytes) -> str:
    return hashlib.sha256(bytes(content or b"")).hexdigest()
