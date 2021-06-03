import base64
from io import BytesIO


def img_to_base64_str(img):
    """
    Take in a Pillow image and convert it to a base64 string in PNG format.
    """
    buffered = BytesIO()
    img.save(buffered, format="PNG")
    buffered.seek(0)
    img_byte = buffered.getvalue()
    img_str = "data:image/png;base64," + base64.b64encode(img_byte).decode()
    return img_str
