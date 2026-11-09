import base64
from io import BytesIO
from app.manifest_name_to_discover_name import name_map
import logging
import os
import requests

def get_dataset_id_from_path(path):
    """
    Get the dataset id from path, we expect the path to be prefix by the dataset id.
    <dataset-id>/files/<path>
    """
    if not path:
        return None
    dataset_id = path.lstrip('/').split('/', 1)[0]
    return dataset_id if dataset_id.isdigit() else None

def getS3BucketNameFromPennsieve(dataset_id, bucket):
    if dataset_id:
        url = f"https://api.pennsieve.io/discover/datasets/{dataset_id}"
        headers = {"accept": "application/json"}
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            uri = response.json().get("uri", "")
            # uri looks like s3://<bucket>/<dataset-id>/
            if uri.startswith("s3://"):
                bucket_name = uri[len("s3://"):].split("/", 1)[0]
                if bucket_name:
                    return bucket_name
        except (requests.exceptions.RequestException, ValueError) as ex:
            logging.error(f"Could not get bucket for dataset {dataset_id}: {ex}")
    return bucket

def replaceOldBucketName(original, bucket):
    outdatedBuckets = [ 'prd-sparc-discover50-use1', 'prd-sparc-discover-use1']
    if original:
        for outdated in outdatedBuckets:
            if outdated in original:
                return original.replace(outdated, bucket)
    return original

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
