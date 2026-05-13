"""
Generate omio.ico  – FcDam-inspired, multi-resolution Windows icon
for OMIO Ultra DB Studio.
Uses only Pillow (no network required – 100 % offline).

Based on FcDam from Icons8 Flat Color Icons (MIT License).
SVG viewBox: 0 0 48 48  –  all coordinates below are in that space.

Layout (48×48 canvas):
  Upper reservoir : rect  x=6  y=10  w=12  h=18   (behind dam, light blue)
  Dam wall        : polygon  (18,6)→(21.2,6)→(25.1,9.2)→(31.5,42)→(18,42)
  Lower reservoir : rect  x=32  y=28  w=10  h=14  (below dam, light blue)
  Waves           : sine arcs in both water areas
"""
from PIL import Image, ImageDraw

# ── exact FcDam / Icons8 Flat Color palette ───────────────────────────────────
WATER    = (129, 212, 250, 255)   # #81D4FA – water fill
WATER_HI = (179, 229, 252, 255)   # #B3E5FC – wave / highlight
DAM_DARK = ( 84, 110, 122, 255)   # #546E7A – concrete dark
DAM_MED  = (120, 144, 156, 255)   # #78909C – concrete mid / edge
CYAN     = (  0, 200, 232, 255)   # #00C8E8 – app accent glow
BG       = ( 13,  27,  42, 255)   # #0D1B2A – app dark background
TRANSP   = (  0,   0,   0,   0)


def _draw_icon(size: int) -> Image.Image:
    """Render the FcDam icon at `size` × `size` pixels."""
    img  = Image.new("RGBA", (size, size), TRANSP)
    draw = ImageDraw.Draw(img)

    # helpers
    s = size / 48.0                                # SVG → pixel scale
    def p(v):  return int(round(v * s))            # scale coordinate
    def pw(v): return max(1, int(round(v * s)))    # scale line width

    # ── rounded dark background ──────────────────────────────────────────────
    r = max(3, p(5))
    draw.rounded_rectangle([0, 0, size - 1, size - 1], radius=r, fill=BG)

    # ── very small sizes: simplified dam silhouette ──────────────────────────
    if size <= 20:
        draw.rectangle([p(6),  p(10), p(18), p(28)], fill=WATER)
        draw.polygon(  [(p(18),p(6)), (p(21),p(6)), (p(25),p(9)),
                        (p(31),p(42)),(p(18),p(42))], fill=DAM_DARK)
        draw.rectangle([p(32), p(28), p(42), p(42)], fill=WATER)
        return img

    # ── upper reservoir (behind / left of dam) ───────────────────────────────
    draw.rectangle([p(6), p(10), p(18), p(28)], fill=WATER)

    # ── dam wall  (exact path from FcDam SVG) ────────────────────────────────
    dam = [
        (p(18.0), p(6.0)),
        (p(21.2), p(6.0)),
        (p(25.1), p(9.2)),
        (p(31.5), p(42.0)),
        (p(18.0), p(42.0)),
    ]
    draw.polygon(dam, fill=DAM_DARK)
    # left vertical edge – lighter face of dam
    draw.line([(p(18), p(6)), (p(18), p(42))], fill=DAM_MED, width=pw(2.0))
    # overflow / spillway channel highlight
    if size >= 32:
        draw.line([(p(20), p(7)), (p(27), p(41))],
                  fill=(*DAM_MED[:3], 140), width=pw(1.2))

    # ── lower reservoir (downstream / right of dam) ──────────────────────────
    draw.rectangle([p(32), p(28), p(42), p(42)], fill=WATER)

    # ── wave ripples – upper water area ─────────────────────────────────────
    if size >= 24:
        for wy in (p(19), p(24)):
            x0, x1 = p(7), p(17)
            hf = (x1 - x0) // 2
            amp = max(1, pw(1.5))
            draw.arc([x0,      wy - amp, x0 + hf, wy + amp],
                     180, 0, fill=WATER_HI, width=pw(0.8))
            draw.arc([x0 + hf, wy - amp, x1,      wy + amp],
                     0, 180, fill=WATER_HI, width=pw(0.8))

    # ── wave ripples – lower water area ─────────────────────────────────────
    if size >= 24:
        for wy in (p(33), p(38)):
            x0, x1 = p(33), p(41)
            hf = (x1 - x0) // 2
            amp = max(1, pw(1.5))
            draw.arc([x0,      wy - amp, x0 + hf, wy + amp],
                     180, 0, fill=WATER_HI, width=pw(0.8))
            draw.arc([x0 + hf, wy - amp, x1,      wy + amp],
                     0, 180, fill=WATER_HI, width=pw(0.8))

    # ── accent glow on dam crest ─────────────────────────────────────────────
    if size >= 32:
        draw.line([(p(18), p(6)), (p(25), p(9.5))], fill=CYAN, width=pw(1.2))
        draw.line([(p(6),  p(10)), (p(18), p(10))], fill=CYAN, width=pw(1.0))

    # ── subtle outer glow ring on large sizes ────────────────────────────────
    if size >= 64:
        glow = Image.new("RGBA", (size, size), TRANSP)
        gd   = ImageDraw.Draw(glow)
        gd.rounded_rectangle([0, 0, size - 1, size - 1],
                              radius=r, outline=(*CYAN[:3], 60), width=pw(1.5))
        img = Image.alpha_composite(img, glow)

    return img


def make_ico(out="omio.ico"):
    sizes  = [256, 128, 64, 48, 32, 24, 16]
    images = [_draw_icon(s) for s in sizes]
    images[0].save(
        out,
        format="ICO",
        sizes=[(s, s) for s in sizes],
        append_images=images[1:],
    )
    print(f"✓ Created {out}  ({len(sizes)} sizes: {sizes})")


if __name__ == "__main__":
    make_ico()
