import base64
from io import BytesIO
from app.config import Config
from app.manifest_name_to_discover_name import name_map
import os

def getS3BucketName(bucketName):
    """
    Guard against old s3 buckets
    """
    outdatedBuckets = [ 'prd-sparc-discover50-use1', 'prd-sparc-discover-use1']
    if bucketName:
        for outdated in outdatedBuckets:
            if outdated in bucketName:
                return bucketName.replace(outdated, Config.DEFAULT_S3_BUCKET_NAME)
    return bucketName

def get_path_from_mangled_list(s3_path):

    # Split out the file part of the path
    s3_paths = s3_path.split('files/')
    file_path = 'files/' + s3_paths[1]  # Add the part we split on back
    # Switch the path to the mapped one if it is listed
    resolved_path = name_map.get(file_path, file_path)
    return s3_paths[0] + resolved_path

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


def print_dict(d, indent=''):
    for k in d.keys():
        print(f'{indent}{k}')
        if isinstance(d[k], dict):
            print_dict(d[k], indent + '  ')
        elif isinstance(d[k], list):
            print(f'{indent}[')
            if len(d[k]) and isinstance(d[k][0], dict):
                print_dict(d[k][0], indent + '  ')
            print(f'{indent}]')


def print_hit_structure(hit):
    print('hit:')
    if isinstance(hit, dict):
        print_dict(hit)

def get_extension(filename: str) -> str:
    """
    Extracts and returns the extension of a filename, including the leading dot.
    If the filename has no extension, returns an empty string.
    """
    _, ext = os.path.splitext(filename)
    return ext
