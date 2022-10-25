from app.scicrunch_processing_common import PASS_THROUGH_KEYS as BASE_PASS_THROUGH_KEYS

PASS_THROUGH_KEYS = ["doi", "dataset_identifier", "dataset_version", "dataset_revision", *BASE_PASS_THROUGH_KEYS]

# attributes is used to map desired parameters onto the path of keys needed in the sci-crunch response.
#  For example:
#  samples: ['attributes','sample','subject'] will find and enter dict keys in the following order:
#  attributes > sample > subject
ATTRIBUTES_MAP = {
    'name': ['item', 'name'],
    'identifier': ['item', 'identifier'],
    'uri': ['distributions', 'current', 'uri'],
    'updated': ['dates', 'updated'],
    'doi': ['item', 'curie'],
    'files': ['objects'],
    'publishDate': ['pennsieve', 'firstPublishedAt', 'timestamp'],
    'dataset_identifier': ['pennsieve', 'identifier'],
    'dataset_version': ['pennsieve', 'version', 'identifier'],
    'dataset_revision': ['pennsieve', 'revision', 'identifier'],
    'description': ['item', 'description'],
    'contributors': ['contributors']
}

def sort_files_by_mime_type(obj_list):
    return {}


def process_result(result):
    output = dict(filter(lambda x: x[0] in PASS_THROUGH_KEYS, result.items()))
    return output
