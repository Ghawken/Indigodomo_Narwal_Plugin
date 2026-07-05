"""Plugin-side map rendering — upscaled floor plan, crisp labels, cleaning trail.

The vendored ``narwal_client.map_renderer`` renders the map at ~1 pixel per grid
cell (tiny) and bakes the room labels in at that resolution, so upscaling makes
the text blocky. This module instead:

  * builds the room floor plan at grid resolution (solid colour blocks),
  * upscales it with integer NEAREST scaling (crisp room edges),
  * then draws the dock, room labels, cleaning trail and robot **after** scaling,
    at full resolution with a properly sized TrueType font, and
  * optionally overlays the robot's cleaned-area grid (experimental).

Kept OUT of ``narwal_client/`` so that package stays a clean, verbatim drop-in
from upstream (see ``narwal_client/VENDORED.md``). It reuses the vendored colour
palette and low-level decode helpers so room colours stay identical.
"""

from __future__ import annotations

import io
import logging
import math
from typing import Any, Sequence

from narwal_client.map_renderer import (
    ROOM_COLORS,
    COLOR_UNKNOWN,
    COLOR_UNASSIGNED_FLOOR,
    COLOR_UNASSIGNED_OBSTACLE,
    COLOR_FALLBACK,
    OBSTACLE_COLORS,
    OBSTACLE_COLOR_DEFAULT,
    _darken,
    decompress_map,
    _decode_packed_varints,
)

_LOGGER = logging.getLogger("Plugin.narwal_map")

# Desired longest edge of the output image, in pixels.
TARGET_PX = 1000
MIN_SCALE = 3
MAX_SCALE = 14

TRAIL_COLOR = (255, 140, 0)          # orange cleaning path
ROBOT_FILL = (255, 45, 45)           # red robot marker
ROBOT_OUTLINE = (255, 255, 255)
DOCK_FILL = (255, 255, 255)
DOCK_OUTLINE = (120, 120, 120)
CLEANED_RGB = (0, 200, 255)          # bright cyan swept tint (alpha supplied per call)
DEFAULT_CLEANED_ALPHA = 150          # ~59% — visible even over blue rooms
# Robot mop/brush is ~250-300mm wide; map resolution is ~60mm/px, so a cleaned
# path cell dilated by 2 (5px diameter) approximates the real swept width.
CLEANED_DILATE = 2

# macOS ships these; Indigo is macOS-only. Falls back to Pillow's default.
_FONT_CANDIDATES = [
    "/System/Library/Fonts/Supplemental/Arial.ttf",
    "/System/Library/Fonts/Helvetica.ttc",
    "/Library/Fonts/Arial.ttf",
    "DejaVuSans.ttf",
    "Arial.ttf",
]


def scale_for(width: int, height: int) -> int:
    """Pick an integer upscale factor so the longest edge ~= TARGET_PX."""
    longest = max(width, height, 1)
    return max(MIN_SCALE, min(MAX_SCALE, max(1, TARGET_PX // longest)))


def _load_font(size: int):
    from PIL import ImageFont
    for path in _FONT_CANDIDATES:
        try:
            return ImageFont.truetype(path, size)
        except (OSError, IOError):
            continue
    try:
        return ImageFont.load_default(size=size)  # Pillow >= 10 (scalable)
    except TypeError:
        return ImageFont.load_default()


def build_base(map_data: Any) -> dict | None:
    """Decode the map and return a dict with the grid-resolution floor plan plus
    room centroids (for post-scale label drawing). Cache this — it only changes
    when the map itself changes."""
    from PIL import Image

    if not map_data or not map_data.compressed_map:
        return None
    w, h = map_data.width, map_data.height
    if w <= 0 or h <= 0:
        return None

    decompressed = decompress_map(map_data.compressed_map)
    if not decompressed:
        return None
    pixels = _decode_packed_varints(decompressed)
    expected = w * h
    if len(pixels) < expected:
        pixels.extend([0] * (expected - len(pixels)))
    elif len(pixels) > expected:
        pixels = pixels[:expected]

    # Transparent background (unknown/outside stays fully transparent) so the PNG
    # can be overlaid on a dashboard; mapped cells are opaque.
    img = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    px = img.load()
    sum_x: dict[int, int] = {}
    sum_y: dict[int, int] = {}
    count: dict[int, int] = {}

    for i, val in enumerate(pixels):
        if val == 0:
            continue  # unknown/outside -> transparent
        x = i % w
        y = i // w
        if val == 0x20:
            px[x, y] = COLOR_UNASSIGNED_FLOOR + (255,)
        elif val == 0x28:
            px[x, y] = COLOR_UNASSIGNED_OBSTACLE + (255,)
        else:
            room_id = val >> 8
            ptype = val & 0xFF
            base = ROOM_COLORS[room_id - 1] if 1 <= room_id <= len(ROOM_COLORS) else COLOR_FALLBACK
            if ptype & 0x10:  # wall/border
                px[x, y] = _darken(base) + (255,)
            else:
                px[x, y] = base + (255,)
                sum_x[room_id] = sum_x.get(room_id, 0) + x
                sum_y[room_id] = sum_y.get(room_id, 0) + y
                count[room_id] = count.get(room_id, 0) + 1

    # Flip so grid Y-up matches image Y-down, same as the vendored renderer.
    img = img.transpose(Image.FLIP_TOP_BOTTOM)
    centroids = {rid: (sum_x[rid] / count[rid], sum_y[rid] / count[rid]) for rid in count}
    room_names = {r.room_id: r.display_name for r in map_data.rooms}
    return {"img": img, "centroids": centroids, "room_names": room_names, "w": w, "h": h}


def _draw_polyline(draw, pts, color, width) -> None:
    try:
        draw.line(pts, fill=color, width=width, joint="curve")
    except TypeError:
        draw.line(pts, fill=color, width=width)


def _draw_label(draw, cx: float, cy: float, text: str, font) -> None:
    bbox = font.getbbox(text)
    tw = bbox[2] - bbox[0]
    th = bbox[3] - bbox[1]
    tx = cx - tw / 2 - bbox[0]
    ty = cy - th / 2 - bbox[1]
    for ox, oy in ((-2, 0), (2, 0), (0, -2), (0, 2), (-2, -2), (2, 2), (-2, 2), (2, -2)):
        draw.text((tx + ox, ty + oy), text, fill=(0, 0, 0), font=font)
    draw.text((tx, ty), text, fill=(255, 255, 255), font=font)


def _render_cleaned_cells(cells, w: int, h: int, out_w: int, out_h: int,
                          alpha: int, dilate: int = 1):
    """Build a translucent, FILLED swept-area layer from cleaned-cell indices.

    ``cells`` is an iterable of linear indices into the ``w`` x ``h`` map grid
    (index = y*w + x), decoded from display_map field 7 — these are the robot's
    path cells (a thin trace), so painted directly they look like stripes/edges.
    We instead build a binary mask and apply a morphological CLOSE (dilate then
    erode) which merges adjacent cleaning passes into solid coverage without
    growing the outer boundary much. ``alpha`` sets the tint opacity.
    """
    from PIL import Image, ImageFilter

    if not cells or w <= 0 or h <= 0:
        return None
    try:
        mask = Image.new("L", (w, h), 0)
        mpx = mask.load()
        wh = w * h
        painted = 0
        minx = miny = 10 ** 9
        maxx = maxy = -1
        for idx in cells:
            if not (0 <= idx < wh):
                continue
            x, y = idx % w, idx // w
            mpx[x, y] = 255
            painted += 1
            minx = min(minx, x); maxx = max(maxx, x)
            miny = min(miny, y); maxy = max(maxy, y)
        if painted == 0:
            _LOGGER.debug("cleaned overlay: 0 in-range cells of %d", len(list(cells)))
            return None

        # Morphological close: dilate (radius 4) then erode (radius 3) to bridge
        # the gaps between passes and fill the swept area into a solid region.
        mask = mask.filter(ImageFilter.MaxFilter(9))
        mask = mask.filter(ImageFilter.MaxFilter(9))
        mask = mask.filter(ImageFilter.MinFilter(7))

        _LOGGER.debug(
            "cleaned overlay: %d cells -> filled mask, grid bbox x[%d..%d] y[%d..%d] (map %dx%d)",
            painted, minx, maxx, miny, maxy, w, h,
        )
        mask = mask.transpose(Image.FLIP_TOP_BOTTOM).resize((out_w, out_h), Image.NEAREST)
        color = (CLEANED_RGB[0], CLEANED_RGB[1], CLEANED_RGB[2], max(0, min(255, int(alpha))))
        solid = Image.new("RGBA", (out_w, out_h), color)
        transparent = Image.new("RGBA", (out_w, out_h), (0, 0, 0, 0))
        return Image.composite(solid, transparent, mask)
    except Exception:
        _LOGGER.debug("cleaned-cell overlay render failed", exc_info=True)
        return None


def compose(
    base: dict | None,
    map_data: Any,
    disp: Any,
    trail: Sequence[tuple[float, float]] | None,
    cleaned=None,
    cleaned_alpha: int = DEFAULT_CLEANED_ALPHA,
    caption: str = "",
) -> bytes:
    """Upscale the cached base map and draw the swept area, trail, dock, labels
    and robot on top at full resolution. Returns PNG bytes, or b"" on failure.

    ``cleaned`` is an optional iterable of cleaned-cell indices (display_map
    field 7) painted as a translucent swept-area layer; ``cleaned_alpha`` sets
    its opacity (0-255)."""
    if not base or not map_data:
        return b""
    from PIL import Image, ImageDraw

    w, h = base["w"], base["h"]
    scale = scale_for(w, h)
    img = base["img"].resize((w * scale, h * scale), Image.NEAREST)  # RGBA, transparent bg
    if img.mode != "RGBA":
        img = img.convert("RGBA")

    def to_img(gx: float, gy: float) -> tuple[float, float]:
        return gx * scale, (h - 1 - gy) * scale

    # Swept-area overlay (drawn first, under trail/labels/robot).
    if cleaned:
        overlay = _render_cleaned_cells(cleaned, w, h, img.width, img.height,
                                        cleaned_alpha, dilate=CLEANED_DILATE)
        if overlay is not None:
            img = Image.alpha_composite(img, overlay)

    draw = ImageDraw.Draw(img)

    # Cleaning trail — split into segments, breaking the line wherever the robot
    # jumped a long way between samples (a travel/teleport move) so we don't draw
    # straight lines across walls.
    if trail and len(trail) >= 2:
        width = max(2, scale // 2)
        gap = max(w, h) * 0.06  # grid cells; a jump larger than this breaks the line
        seg = [to_img(*trail[0])]
        for i in range(1, len(trail)):
            px_, py_ = trail[i]
            lx, ly = trail[i - 1]
            if ((px_ - lx) ** 2 + (py_ - ly) ** 2) ** 0.5 > gap:
                if len(seg) >= 2:
                    _draw_polyline(draw, seg, TRAIL_COLOR, width)
                seg = [to_img(px_, py_)]
            else:
                seg.append(to_img(px_, py_))
        if len(seg) >= 2:
            _draw_polyline(draw, seg, TRAIL_COLOR, width)

    # Dock.
    if map_data.dock_x is not None and map_data.dock_y is not None:
        dx, dy = to_img(map_data.dock_x, map_data.dock_y)
        dr = max(5, scale * 2)
        draw.ellipse([dx - dr, dy - dr, dx + dr, dy + dr], fill=DOCK_FILL,
                     outline=DOCK_OUTLINE, width=2)

    # Furniture / obstacle annotations (static, from get_map field 2.32).
    obstacles = getattr(map_data, "obstacles", None)
    if obstacles:
        ofont = _load_font(max(10, min(24, scale * 2)))
        for obs in obstacles:
            try:
                gx, gy = obs.to_grid_coords(map_data.origin_x, map_data.origin_y)
            except Exception:
                continue
            if not (0 <= gx < w and 0 <= gy < h):
                continue
            cx, cy = to_img(gx, gy)
            hw = max(3, (obs.width / 2.0) * scale)
            hh = max(3, (obs.height / 2.0) * scale)
            color = OBSTACLE_COLORS.get(obs.type_id, OBSTACLE_COLOR_DEFAULT)
            draw.rectangle([cx - hw, cy - hh, cx + hw, cy + hh],
                           outline=color, width=max(1, scale // 3))
            label = obs.display_name
            if label:
                lb = ofont.getbbox(label)
                lx = cx - (lb[2] - lb[0]) / 2 - lb[0]
                ly = cy - hh - (lb[3] - lb[1]) - 2 * scale - lb[1]
                for ox, oy in ((-1, 0), (1, 0), (0, -1), (0, 1)):
                    draw.text((lx + ox, ly + oy), label, fill=(0, 0, 0), font=ofont)
                draw.text((lx, ly), label, fill=color, font=ofont)

    # Room labels — drawn post-scale with a properly sized font (crisp).
    font = _load_font(max(12, min(40, scale * 3)))
    for rid, (gx, gy) in base["centroids"].items():
        name = base["room_names"].get(rid)
        if name:
            cx, cy = to_img(gx, gy)
            _draw_label(draw, cx, cy, name, font)

    # Robot marker + heading arrow (on top).
    if disp is not None:
        gc = disp.to_grid_coords(map_data.resolution, map_data.origin_x, map_data.origin_y)
        if gc is not None:
            gx, gy = gc
            if 0 <= gx < w and 0 <= gy < h:
                cx, cy = to_img(gx, gy)
                r = max(5, scale * 2)
                draw.ellipse([cx - r, cy - r, cx + r, cy + r], fill=ROBOT_FILL,
                             outline=ROBOT_OUTLINE, width=2)
                heading = disp.robot_heading
                if heading is not None:
                    rad = math.radians(heading)
                    hx = cx + math.cos(rad) * r * 1.8
                    hy = cy - math.sin(rad) * r * 1.8
                    draw.line([cx, cy, hx, hy], fill=ROBOT_OUTLINE, width=max(2, scale // 2))

    # Caption (burned-in timestamp + stats) so freshness is visible when the PNG
    # is opened directly in a viewer that caches. Bottom-left, dark backing.
    if caption:
        cfont = _load_font(max(16, scale * 5))
        bbox = draw.textbbox((0, 0), caption, font=cfont)
        tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
        pad = max(6, scale * 2)
        x0, y0 = pad, img.height - th - 3 * pad
        draw.rectangle([x0 - pad, y0 - pad, x0 + tw + pad, y0 + th + pad], fill=(0, 0, 0))
        draw.text((x0 - bbox[0], y0 - bbox[1]), caption, fill=(255, 255, 255), font=cfont)

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()
