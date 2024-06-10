import logging

from app import Config
from app.scicrunch_processing_common import map_mime_type, COMMON_IMAGES, NOT_SPECIFIED, SKIP
from app.scicrunch_processing_common import PASS_THROUGH_KEYS as BASE_PASS_THROUGH_KEYS
from app.manifest_name_to_discover_name import name_map

PASS_THROUGH_KEYS = ["doi", "dataset_identifier", "dataset_version", "dataset_revision", 's3uri', *BASE_PASS_THROUGH_KEYS]


# attributes is used to map desired parameters onto the path of keys needed in the sci-crunch response.
#  For example:
#  sampleSiz: ['item', 'statistics', 'sample', 'count'] will find and enter dict keys in the following order:
#  item > statistics > samples > count
ATTRIBUTES_MAP = {
    'additionalLinks': ['xrefs', 'additionalLinks'],
    'sampleSize': ['item', 'statistics', 'samples', 'count'],
    'subjectSize': ['item', 'statistics', 'subjects', 'count'],
    'name': ['item', 'name'],
    'description': ['item', 'description'],
    'identifier': ['item', 'identifier'],
    'uri': ['distributions', 'current', 'uri'],
    'updated': ['dates', 'updated'],
    'organs': ['anatomy', 'organ'],
    'organisms': ['organisms', 'subject'],
    'contributors': ['contributors'],
    'doi': ['item', 'curie'],
    'files': ['objects'],
    'version': ['item', 'version', 'keyword'],
    's3uri': ['pennsieve', 'uri'],
    'publishDate': ['pennsieve', 'firstPublishedAt', 'timestamp'],
    'dataset_identifier': ['pennsieve', 'identifier'],
    'dataset_version': ['pennsieve', 'version', 'identifier'],
    'dataset_revision': ['pennsieve', 'revision', 'identifier'],
}


def sort_files_by_mime_type(obj_list):
    sorted_files = {}
    if not obj_list:
        return sorted_files

    for obj in obj_list:

        mime_type = obj.get('additional_mimetype', NOT_SPECIFIED)
        if mime_type != NOT_SPECIFIED:
            mime_type = mime_type.get('name')

        if not mime_type:
            mime_type = obj['mimetype'].get('name', NOT_SPECIFIED)

        mapped_mime_type = map_mime_type(mime_type, obj)
        if mapped_mime_type == NOT_SPECIFIED:
            logging.warning(f'Unhandled mime type: {mime_type}')
        elif mapped_mime_type == SKIP:
            pass
        else:
            if 'dataset' in obj and 'path' in obj['dataset']:
                dataset_path_prefix = 'files/'
                dataset_path = dataset_path_prefix + obj['dataset']['path']
                if dataset_path in name_map:
                    obj['dataset']['path'] = name_map[dataset_path].replace(dataset_path_prefix, '', 1)

            if mapped_mime_type in sorted_files:
                sorted_files[mapped_mime_type].append(obj)
            else:
                sorted_files[mapped_mime_type] = [obj]

    return sorted_files


def process_result(result):
    output = dict(filter(lambda x: x[0] in PASS_THROUGH_KEYS, result.items()))
    if COMMON_IMAGES in result:
        output[COMMON_IMAGES] = []
        for common_image in result[COMMON_IMAGES]:
            if 'bytes' in common_image and int(common_image['bytes']['count']) < Config.DIRECT_DOWNLOAD_LIMIT:
                output[COMMON_IMAGES].append(common_image)

    return output
