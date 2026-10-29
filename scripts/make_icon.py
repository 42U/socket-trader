"""SocketTrader desktop icon: a dollar sign in the digital-rain idiom.
Rendered at 1024 px then downsampled to every size Windows asks for."""
import random
from PIL import Image, ImageDraw, ImageFont, ImageFilter

S = 1024
rng = random.Random(7)
MONO = "/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf"
MONO_R = "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"

img = Image.new("RGBA", (S, S), (0, 0, 0, 0))
d = ImageDraw.Draw(img)
# Rounded dark tile (so it reads as an app icon on any desktop background).
r = 190
d.rounded_rectangle((0, 0, S - 1, S - 1), radius=r, fill=(7, 11, 16, 255))
# Subtle vignette toward the centre so the glyph sits in a pool of light.
glow = Image.new("RGBA", (S, S), (0, 0, 0, 0))
gd = ImageDraw.Draw(glow)
gd.ellipse((S*0.18, S*0.18, S*0.82, S*0.82), fill=(20, 90, 60, 150))
glow = glow.filter(ImageFilter.GaussianBlur(140))
img.alpha_composite(glow)

# Digital rain: columns of small glyphs, bright heads, fading tails.
rain = Image.new("RGBA", (S, S), (0, 0, 0, 0))
rd = ImageDraw.Draw(rain)
G = "01ABCDEFGHJKLMNPQRSTUVWXYZ23456789$#@%&+=<>"
cell = 44
f_small = ImageFont.truetype(MONO_R, 40)
for col in range(S // cell + 1):
    x = col * cell + 6
    if rng.random() < 0.28:
        continue
    head = rng.randint(0, S // cell)
    length = rng.randint(5, 14)
    for k in range(length):
        row = head - k
        y = row * cell
        if y < -cell or y > S:
            continue
        ch = rng.choice(G)
        if k == 0:
            colr = (210, 255, 230, 235)
        else:
            a = int(200 * (1 - k / length) ** 1.4)
            colr = (47, 191, 132, a)
        rd.text((x, y), ch, font=f_small, fill=colr)
# Keep the rain quieter where the dollar sits, so the mark stays legible.
mask = Image.new("L", (S, S), 255)
md = ImageDraw.Draw(mask)
md.ellipse((S*0.22, S*0.14, S*0.78, S*0.86), fill=70)
mask = mask.filter(ImageFilter.GaussianBlur(60))
rain.putalpha(Image.eval(rain.getchannel("A"), lambda a: a).point(lambda v: v) )
ra = rain.getchannel("A")
ra = Image.composite(ra, Image.eval(ra, lambda v: v * 70 // 255), mask.point(lambda v: 255 if v > 200 else 0))
# simpler: multiply alpha by mask/255
ra = Image.fromarray((__import__("numpy").array(rain.getchannel("A"), dtype="float32") *
                      (__import__("numpy").array(mask, dtype="float32") / 255.0)).astype("uint8"))
rain.putalpha(ra)
# clip the rain to the rounded tile
tile = Image.new("L", (S, S), 0)
ImageDraw.Draw(tile).rounded_rectangle((0, 0, S - 1, S - 1), radius=r, fill=255)
rain.putalpha(Image.fromarray((__import__("numpy").array(rain.getchannel("A"), dtype="float32") *
                               (__import__("numpy").array(tile, dtype="float32") / 255.0)).astype("uint8")))
img.alpha_composite(rain)

# The dollar: bold mono glyph, green with a white-hot core and a soft bloom.
f_big = ImageFont.truetype(MONO, 760)
bbox = d.textbbox((0, 0), "$", font=f_big)
tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
pos = ((S - tw) // 2 - bbox[0], (S - th) // 2 - bbox[1] - 10)
bloom = Image.new("RGBA", (S, S), (0, 0, 0, 0))
ImageDraw.Draw(bloom).text(pos, "$", font=f_big, fill=(47, 191, 132, 255))
bloom = bloom.filter(ImageFilter.GaussianBlur(28))
img.alpha_composite(bloom)
img.alpha_composite(bloom)
ImageDraw.Draw(img).text(pos, "$", font=f_big, fill=(47, 191, 132, 255))
core = Image.new("RGBA", (S, S), (0, 0, 0, 0))
ImageDraw.Draw(core).text(pos, "$", font=f_big, fill=(232, 255, 243, 255))
# inner core: erode by blurring the alpha and thresholding high
ca = core.getchannel("A").filter(ImageFilter.GaussianBlur(14)).point(lambda v: 255 if v > 215 else 0)
core.putalpha(ca)
img.alpha_composite(core)
# Scanlines, faint, for the CRT feel.
sl = Image.new("RGBA", (S, S), (0, 0, 0, 0))
sd = ImageDraw.Draw(sl)
for y in range(0, S, 6):
    sd.line((0, y, S, y), fill=(0, 0, 0, 38))
sl.putalpha(Image.fromarray((__import__("numpy").array(sl.getchannel("A"), dtype="float32") *
                             (__import__("numpy").array(tile, dtype="float32") / 255.0)).astype("uint8")))
img.alpha_composite(sl)

img.save("sockettrader_1024.png")
sizes = [256, 128, 64, 48, 32, 24, 16]
img.save("sockettrader.ico", sizes=[(s, s) for s in sizes])
# preview strip
strip = Image.new("RGBA", (sum(sizes) + 20 * len(sizes), 270), (40, 40, 40, 255))
x = 10
for s_ in sizes:
    im = img.resize((s_, s_), Image.LANCZOS)
    strip.alpha_composite(im, (x, 10)); x += s_ + 20
strip.save("preview.png")
print("ok")
