import sys
import glob
import os.path
from PIL import Image


def convert(filepath: str) -> str:
    if '.output' in filepath:
        return None
    filename, ext = os.path.splitext(filepath)
    output = filename + '.output.jpeg'
    if 'weixin' in filepath:
        crop_size = (270, 420, 270 + 580, 420 + 580)
    elif 'alipay' in filepath:
        crop_size = (202, 561, 202 + 675, 561 + 675)
    else:
        return None
    img = Image.open(filepath)
    if img.size[0] <= 320 or img.size[1] <= 320:
        return None
    img.crop(crop_size)\
        .convert('RGB')\
        .resize((320, 320), Image.HAMMING)\
        .save(output)
    return output


def main():
    if len(sys.argv) < 2:
        print('Usage: convert_qrcode.py <file> ...')
        return
    for pathname in sys.argv[1:]:
        for filepath in glob.glob(pathname):
            output = convert(filepath)
            if output:
                print(output)


if __name__ == "__main__":
    main()
