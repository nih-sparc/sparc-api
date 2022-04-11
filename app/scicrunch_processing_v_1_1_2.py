import logging

from app import Config
from app.scicrunch_processing_common import SEGMENTATION_FILES, COMMON_IMAGES, NOT_SPECIFIED, SKIP, PASS_THROUGH_KEYS, PLOT_FILE, THUMBNAIL_IMAGE, SCAFFOLD_FILE, SCAFFOLD_DIR, \
    VIDEO, BIOLUCIDA_2D, BIOLUCIDA_3D, CSV

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
    'samples': ['attributes', 'sample', 'subject'],
    'name': ['item', 'name'],
    'identifier': ['item', 'identifier'],
    'uri': ['distributions', 'current', 'uri'],
    'updated': ['dates', 'updated'],
    'organs': ['anatomy', 'organ'],
    'contributors': ['contributors'],
    'doi': ['item', 'curie'],
    'files': ['objects'],
    'version': ['item', 'version', 'keyword']
}

_NEUROLUCIDA_FUDGES = [
    # The file below is from dataset 37.
    'package:b15f0795-9a5a-4a6c-9633-d843bcaf5983/files/1155468',
    # The files below are from dataset 64.
    'package:40181d25-fb4a-40c1-8778-c7425ae0b680/files/1106299',
    'package:c882893d-c2c6-47a1-b907-85eb4ffa8cb8/files/550966',
    'package:49eeb3ec-6eb0-427d-8b27-1e871f6cc13f/files/1155473',
    # 'package:fee3f032-cb0b-4d81-b563-4faa32791be7/files/1155472',
    'package:3935b9dd-6135-490c-8e17-809747670b81/files/1106335',
    # 'package:171775cd-5d11-4157-a49a-dbeee7e083a6/files/1155471',
    'package:01219a1c-c05f-402a-9316-073c6a774820/files/1155467',
    # 'package:0cbd1568-a29b-4f8e-b042-b3d338200498/files/1155474',
    # 'package:904323c9-0fd4-44de-b490-95a0e0bb90c0/files/1179673',
    'package:8b6ca367-02fe-4b96-8d57-5ac14bb782fb/files/1106379',
    # 'package:1c944008-4612-42fc-9fcf-4c03911a490a/files/1155475',
    'package:a4db43f7-381a-4cd8-9317-e5e61aee7193/files/1155470',
    'package:e835a8e2-d5be-4fa6-8388-2147c2f0a610/files/1155469',
    'package:6f4230ca-d435-4a4b-af8e-492330f40d4c/files/1155466',
    'package:284ac044-a1ea-46f0-9583-c36f6beaa1e6/files/1106306',
    'package:c83ec8f6-880b-491d-a3e5-c6e47060cb24/files/1155463',
    'package:57dcdc0f-880f-4221-8752-ce1227cb032d/files/1155465',
    'package:3b2d9ce4-2563-4959-8ab5-d007cd058313/files/1155464'
]


def _fudge_object(obj):
    # if obj['remote']['id'] in _NEUROLUCIDA_FUDGES:
    if obj['identifier']:
        obj['mimetype'] = 'application/vnd.mbfbioscience.neurolucida+xml'
    if obj.get('mimetype', 'none') == 'application/json' and "metadata.json" in obj.get('dataset', 'none')['path']:
        obj['mimetype'] = 'inode/vnd.abi.scaffold+file'

    return obj


def _mapped_mime_type(mime_type, obj):
    mapped_mime_types = {
        'text/csv': CSV,
        'application/vnd.mbfbioscience.metadata+xml': SEGMENTATION_FILES,
        'application/vnd.mbfbioscience.neurolucida+xml': SEGMENTATION_FILES,
        'inode/vnd.abi.scaffold+directory': SCAFFOLD_DIR,
        'inode/vnd.abi.scaffold+file': SCAFFOLD_FILE,
        'inode/vnd.abi.scaffold+thumbnail': THUMBNAIL_IMAGE,
        'text/vnd.abi.plot+Tab-separated-values': PLOT_FILE,
        'text/vnd.abi.plot+csv': PLOT_FILE,
        'image/png': COMMON_IMAGES,
        'image/tiff': 'tiff-image',
        'image/tif': 'tiff-image',
        'image/jpeg': COMMON_IMAGES,
        'image/jpx': BIOLUCIDA_3D,
        'image/jp2': BIOLUCIDA_2D,
        'video/mp4': VIDEO
    }
    skipped_mime_types = [
        'application/json',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        'application/vnd.openxmlformats-officedocument.presentationml.presentation',
        'chemical/x-gamess-input',
        'application/pdf',
        'application/zip',
        'application/xml',
        'inode/directory',
        'application/vnd.oasis.opendocument.database',
        'text/plain',
        'text/html',
        'text/css',
        'text/x-python',
        'text/x-chdr',
        'text/x-c++src',
        'text/markdown',
        'text/Tab-separated-values',
        'audio/midi',
        'text/x-sh',
        'image/vnd.zeiss.czi',
        'image/vnd.nikon.nd2',
        'image/vnd.ome.xml+tiff',
        'image/vnd.ome.xml+jp2',
        'image/vnd.ome.xml+jpx',
        'image/x-coreldraw',
        'application/x-matlab',
        'application/x-matlab-data',
        'application/vnd.cambridge-electronic-designced.spike2.64.data',
        'application/vnd.cambridge-electronic-designced.spike2.32.data',
        'application/vnd.cambridge-electronic-designced.spike2.resource+xml',
        'application/octet-stream',
        'application/x-bzip-compressed-fastq',
        'text/tab-separated-values',
        'image/gznii',
        'application/x-tar',
        'video/x-msvideo',
        'application/vnd.ms-excel',
        'application/x-msdos-program',
        'chemical/x-ncbi-asn1-ascii',
        'text/h323',
        'application/rar',
        'application/x-gz-compressed-fastq',
        'application/fastq',
    ]
    if mime_type == '':
        return SKIP

    if mime_type == NOT_SPECIFIED:
        return SKIP

    if mime_type in skipped_mime_types:
        return SKIP

    if mime_type in mapped_mime_types:
        if mime_type in ["image/jpeg", "image/png"]:
            try:
                if obj['dataset']['path'].startswith('derivative'):
                    return SKIP
            except KeyError:
                return SKIP
        return mapped_mime_types[mime_type]

    return NOT_SPECIFIED


def sort_files_by_mime_type(obj_list):
    sorted_files = {}
    if not obj_list:
        return sorted_files

    for obj in obj_list:
        # Hacks are applied here.
        # obj = _fudge_object(obj)

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
            if mapped_mime_type in sorted_files:
                sorted_files[mapped_mime_type].append(obj)
            else:
                sorted_files[mapped_mime_type] = [obj]

    return sorted_files


def process_result(result):
    # print('=====================')
    # if isinstance(result['files'], list):
    #     print(len(result['files']))
    # else:
    #     print('what is this?', result['files'])

    # for f in result['files']:
    #     if 'additional_mimetype' in f:
    #         print(f['additional_mimetype'])
    output = dict(filter(lambda x: x[0] in PASS_THROUGH_KEYS, result.items()))
    if COMMON_IMAGES in result:
        output[COMMON_IMAGES] = []
        for common_image in result[COMMON_IMAGES]:
            if 'bytes' in common_image and int(common_image['bytes']['count']) < Config.DIRECT_DOWNLOAD_LIMIT:
                output[COMMON_IMAGES].append(common_image)

    return output
