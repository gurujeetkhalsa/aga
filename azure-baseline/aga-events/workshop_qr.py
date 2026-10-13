"""Scannable workshop promotion badge, with wording outside the QR quiet zone."""
from io import BytesIO
import math
from xml.sax.saxutils import escape

import qrcode
from PIL import Image, ImageDraw, ImageFont

REGISTRATION_URL = 'https://aga-events.azurewebsites.net/events/learn-go-in-an-hour'
TOP_TEXT = 'Free Beginners Online Workshop'
BOTTOM_TEXT = 'Learn Go in an hour - Register here.'
CAPTION = f'{TOP_TEXT} {BOTTOM_TEXT}'
SIZE = 1200
DOWNLOAD_SIZE = 360
RED = '#a71920'


def _matrix(target):
    qr = qrcode.QRCode(error_correction=qrcode.constants.ERROR_CORRECT_Q, border=4)
    qr.add_data(target)
    qr.make(fit=True)
    return qr.get_matrix()


def _geometry(matrix):
    module = 624 // len(matrix)
    side = module * len(matrix)
    return module, (SIZE - side) // 2


def badge_svg(target=REGISTRATION_URL, *, test_label=False, size=SIZE):
    matrix = _matrix(target)
    module, offset = _geometry(matrix)
    cells = ' '.join(f'M{offset+x*module} {offset+y*module}h{module}v{module}h-{module}z'
                     for y, row in enumerate(matrix) for x, dark in enumerate(row) if dark)
    label=(f'<rect x="465" y="205" width="270" height="76" rx="12" fill="{RED}"/>'
           '<text x="600" y="259" text-anchor="middle" font-family="Arial, Helvetica, sans-serif" font-weight="700" font-size="50" fill="white">TEST</text>') if test_label else ''
    return f'''<svg xmlns="http://www.w3.org/2000/svg" width="{size}" height="{size}" viewBox="0 0 1200 1200" role="img" aria-labelledby="title desc">
<title id="title">{escape(('TEST - ' if test_label else '') + CAPTION)}</title>
<desc id="desc">Scan to register: {escape(target)}</desc>
<rect width="1200" height="1200" fill="white"/>
<circle cx="600" cy="600" r="574" fill="none" stroke="{RED}" stroke-width="3"/>
<defs>
<path id="top" d="M100 600 A500 500 0 0 1 1100 600"/>
<path id="bottom" d="M100 600 A500 500 0 0 0 1100 600"/>
</defs>
<g fill="{RED}" font-family="Arial, Helvetica, sans-serif" font-weight="700" font-size="54" text-anchor="middle">
<text><textPath href="#top" startOffset="50%">{escape(TOP_TEXT)}</textPath></text>
<text><textPath href="#bottom" startOffset="50%">{escape(BOTTOM_TEXT)}</textPath></text>
</g>
<path d="{cells}" fill="black" shape-rendering="crispEdges"/>
{label}
</svg>'''.encode('utf-8')


def _arc_text(image, text, top):
    font = ImageFont.load_default(size=54)
    widths = [font.getlength(char) for char in text]
    radius = 500
    arc = sum(widths) / radius
    start = -math.pi/2-arc/2 if top else math.pi/2+arc/2
    travelled = 0
    for char, width in zip(text, widths):
        angle = start + (travelled+width/2)/radius * (1 if top else -1)
        travelled += width
        glyph = Image.new('RGBA', (120, 140))
        ImageDraw.Draw(glyph).text((60,70), char, font=font, fill=RED, anchor='mm')
        rotation = -math.degrees(angle)-(90 if top else -90)
        glyph = glyph.rotate(rotation, resample=Image.Resampling.BICUBIC, expand=True)
        x = SIZE/2+radius*math.cos(angle)
        y = SIZE/2+radius*math.sin(angle)
        image.paste(glyph, (round(x-glyph.width/2),round(y-glyph.height/2)), glyph)


def badge_png(target=REGISTRATION_URL, *, test_label=False, size=SIZE):
    matrix = _matrix(target)
    module, offset = _geometry(matrix)
    image = Image.new('RGB', (SIZE,SIZE), 'white')
    draw = ImageDraw.Draw(image)
    draw.ellipse((26,26,1174,1174), outline=RED, width=3)
    _arc_text(image, TOP_TEXT, True)
    _arc_text(image, BOTTOM_TEXT, False)
    if test_label:
        draw.rounded_rectangle((465,205,735,281),radius=12,fill=RED)
        draw.text((600,243),'TEST',font=ImageFont.load_default(size=50),fill='white',anchor='mm')
    for y,row in enumerate(matrix):
        for x,dark in enumerate(row):
            if dark:
                left,top=offset+x*module,offset+y*module
                draw.rectangle((left,top,left+module-1,top+module-1),fill='black')
    if size != SIZE:
        image=image.resize((size,size),Image.Resampling.LANCZOS)
    buffer=BytesIO()
    image.save(buffer,format='PNG',dpi=(300,300))
    return buffer.getvalue()


if __name__ == '__main__':
    from pathlib import Path
    import argparse
    parser=argparse.ArgumentParser()
    parser.add_argument('--test',action='store_true',help='Generate labeled rehearsal QR codes.')
    args=parser.parse_args()
    target=Path(__file__).parent/'downloads'
    target.mkdir(exist_ok=True)
    url=REGISTRATION_URL+'-rehearsal' if args.test else REGISTRATION_URL
    name='test-free-beginners-workshop-qr' if args.test else 'free-beginners-workshop-qr'
    (target/(name+'.svg')).write_bytes(badge_svg(url,test_label=args.test))
    (target/(name+'.png')).write_bytes(badge_png(url,test_label=args.test))
    print(f'Created PNG and SVG badges in {target}.')
