import logging

from app import Config
from app.scicrunch_processing_common import COMMON_IMAGES, NOT_SPECIFIED, SKIP, PASS_THROUGH_KEYS, SKIPPED_MIME_TYPES, MAPPED_MIME_TYPES
from app.manifest_name_to_discover_name import name_map

# Hit layout:
# _source
#   item
#     version
#       keyword
#     types
#     [
#       curie
#       name
#       type
#     ]
#     contentTypes
#     [
#       curie
#       name
#     ]
#     names
#     [
#       nameType
#       name
#     ]
#     statistics
#       files
#         count
#       directory
#         count
#       bytes
#         count
#     keywords
#     [
#       keyword
#     ]
#     published
#       status
#       boolean
#     description
#     name
#     techniques
#     [
#       keyword
#     ]
#     readme
#       description
#     identifier
#     docid
#     curie
#     modalities
#     [
#       keyword
#     ]
#   supportingAwards
#   [
#     identifier
#     agency
#       name
#   ]
#   objects
#   [
#     identifier
#     bytes
#       count
#     name
#     mimetype
#       name
#     updated
#       timestamp
#     dataset
#       identifier
#       path
#     distributions
#       current
#       [
#         uri
#       ]
#       api
#       [
#         uri
#       ]
#   ]
#   contributors
#   [
#     name
#   ]
#   dates
#     created
#       timestamp
#     updated
#     [
#       timestamp
#     ]
#   protocols
#     primary
#     [
#       curie
#       name
#       uri
#     ]
#   anatomy
#     organ
#     [
#       curie
#       name
#       matchingStatus
#     ]
#   distributions
#     current
#     [
#       uri
#     ]
#     api
#     [
#       uri
#     ]
#   publication
#     originating
#     [
#       curie
#       uri
#     ]
#   dataItem
#     dataTypes
#     [
#     ]
#   provenance
#     ingestMethod
#     ingestTarget
#     filePattern
#     ingestTime
#     creationDate
#     [
#     ]
#     docId
#     primaryKey
#   organization
#     hierarchy


# attributes is used to map desired parameters onto the path of keys needed in the sci-crunch response.
#  For example:
#  samples: ['attributes','sample','subject'] will find and enter dict keys in the following order:
#  attributes > sample > subject
ATTRIBUTES_MAP = {
    'additionalLinks': ['xrefs', 'additionalLinks'],
    'scaffolds': ['scaffolds'],
    'samples': ['attributes', 'sample', 'subject'],
    'name': ['item', 'name'],
    'description': ['item','description'],
    'identifier': ['item', 'identifier'],
    'uri': ['distributions', 'current', 'uri'],
    'updated': ['dates', 'updated'],
    'organs': ['anatomy', 'organ'],
    'organisms': ['organisms', 'subject'],
    'contributors': ['contributors'],
    'doi': ['item', 'curie'],
    'files': ['objects'],
    'version': ['item', 'version', 'keyword'],
    's3uri': ['pennsieve', 'uri']
}


def _mapped_mime_type(mime_type, obj):

    if mime_type == '':
        return SKIP

    if mime_type == NOT_SPECIFIED:
        return SKIP

    if mime_type in SKIPPED_MIME_TYPES:
        return SKIP

    if mime_type in MAPPED_MIME_TYPES:
        if mime_type in ["image/jpeg", "image/png"]:
            try:
                if obj['dataset']['path'].startswith('derivative'):
                    return SKIP
            except KeyError:
                return SKIP
        return MAPPED_MIME_TYPES[mime_type]

    return NOT_SPECIFIED


def sort_files_by_mime_type(obj_list):
    sorted_files = {}
    if not obj_list:
        return sorted_files

    for obj in obj_list:

        mime_type = obj.get('additional_mimetype', NOT_SPECIFIED)
        if mime_type != NOT_SPECIFIED:
            mime_type = mime_type.get('name')
        else:
            mime_type = obj['mimetype'].get('name', NOT_SPECIFIED)

        mapped_mime_type = _mapped_mime_type(mime_type, obj)
        if mapped_mime_type == NOT_SPECIFIED:
            logging.warning('Unhandled mime type:', mime_type)
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
