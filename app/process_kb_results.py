import json
import logging

from app.config import Config

_NOT_SPECIFIED = 'not-specified'
_SKIP = 'skip'
_COMMON_IMAGES = 'common-images'

# attributes is used to map desired parameters onto the path of keys needed in the sci-crunch response.
#  For example:
#  samples: ['attributes','sample','subject'] will find and enter dict keys in the following order:
#  attributes > sample > subject
attributes = {
    'scaffolds': ['scaffolds'],
    'samples': ['attributes', 'sample', 'subject'],
    'name': ['item', 'name'],
    'identifier': ['item', 'identifier'],
    'uri': ['distributions', 'current', 'uri'],
    'updated': ['dates', 'updated'],
    'organs': ['anatomy', 'organ'],
    'contributors': ['contributors'],
    'doi': ['item', 'curie'],
    'files': ['objects']
}


# create_facet_query(type): Generates facet search request data for sci-crunch  given a 'type'; where
# 'type' is either 'species', 'gender', or 'genotype' at this stage.
#  Returns a tuple of the typemap and request data ( type_map, data )
def create_facet_query(type_):
    type_map = {
        'species': ['organisms.primary.species.name.aggregate', 'organisms.sample.species.name.aggregate'],
        'gender': ['attributes.subject.sex.value'],
        'genotype': ['anatomy.organ.name.aggregate']
    }

    data = {
        "from": 0,
        "size": 0,
        "aggregations": {
            f"{type_}": {
                "terms": {
                    "field": "",
                    "size": 200,
                    "order": [
                        {
                            "_count": "desc"
                        },
                        {
                            "_key": "asc"
                        }
                    ]
                }
            }
        }
    }

    return type_map, data


# create_facet_query(query, terms, facets, size, start): Generates filter search request data for sci-crunch
#  All inputs to facet query have defaults defined as 'None' (this is done so we can directly take in URL params
#  as input).
#  Returns a json query to be used in a sci-crunch request as request json data
def create_filter_request(query, terms, facets, size, start):
    if size is None:
        size = 10
    if start is None:
        start = 0

    if query == "" and len(terms) == 0 and len(facets) == 0:
        return {"size": size, "from": start}

    # Type map is used to map scicrunch paths to given facet
    type_map = {
        'species': ['organisms.primary.species.name.aggregate', 'organisms.sample.species.name'],
        'gender': ['attributes.subject.sex.value', 'attributes.sample.sex.value'],
        'genotype': ['anatomy.organ.name.aggregate']
    }

    # Data structure of a sci-crunch search
    data = {
      "size": size,
      "from": start,
      "query": {
          "query_string": {
              "query": ""
          }
      }
    }

    qs = facet_query_string(query, terms, facets, type_map)
    data["query"]["query_string"]["query"] = qs

    return data


def facet_query_string(query, terms, facets, type_map):

    # We will create AND OR structure. OR within facets and AND between them
    # Example Output:
    #
    # "heart AND attributes.subject.sex.value:((male) OR (female))"

    t = {}
    for i, term in enumerate(terms):
        if term is None or facets[i] is None or 'All' in facets[i]:  # Ignore 'All species' facets
            continue
        else:
            if term not in t.keys():  # If term hasn't been seen, add it to the list of terms
                t[term] = [facets[i]]
            else:
                t[term].append(facets[i])  # If term has been seen append it to it's term

    # Add search query if it exists
    qt = ""
    if query != "":
        qt = f'({query})'

    if query != "" and len(t) > 0:
        qt += " AND "

    # Add the brackets and OR and AND parameters
    for k in t:
        qt += type_map[k][0] + ":("  # facet term path and opening bracket
        for l in t[k]:
            qt += f"({l})"  # bracket around terms incase there are spaces
            if l is not t[k][-1]:
                qt += " OR "  # 'OR' if more terms in this facet are coming
            else:
                qt += ") "

        if k is not list(t.keys())[-1]:  # Add 'AND' if we are not at the last item
                qt += " AND "
    return qt


# process_kb_results: Loop through SciCrunch results pulling out desired attributes and processing DOIs and CSV files

def _prepare_kb_results(results):
    output = []
    hits = results['hits']['hits']
    for i, hit in enumerate(hits):
        attr = _get_attributes(attributes, hit)
        attr['doi'] = _convert_doi_to_url(attr['doi'])
        attr.update(_sort_files_by_mime_type(attr['files']))

        output.append(_manipulate_attr(attr))

    return output


def process_kb_results(results):
    return json.dumps({'numberOfHits': results['hits']['total'], 'results': _prepare_kb_results(results)})


def _process_kb_result(result):
    output = {}
    if _COMMON_IMAGES in result:
        output[_COMMON_IMAGES] = []
        for common_image in result[_COMMON_IMAGES]:
            if int(common_image['bytes']['count']) < Config.DIRECT_DOWNLOAD_LIMIT:
                output[_COMMON_IMAGES].append(common_image)

    return output


def dataset_results(results):
    processed_outputs = []
    kb_results = _prepare_kb_results(results)
    for kb_result in kb_results:
        processed_outputs.append(_process_kb_result(kb_result))

    return {'result': processed_outputs}


def _convert_doi_to_url(doi):
    if not doi:
        return doi
    return doi.replace('DOI:', 'https://doi.org/')


def _mapped_mime_type(mime_type, obj):
    mapped_mime_types = {
        'text/csv': 'csv',
        'application/vnd.mbfbioscience.metadata+xml': 'mbf-segmentation',
        'application/vnd.mbfbioscience.neurolucida+xml': 'mbf-segmentation',
        'inode/vnd.abi.scaffold+directory': 'abi-scaffold-dir',
        'inode/vnd.abi.scaffold+file': 'abi-scaffold-file',
        'inode/vnd.abi.scaffold+thumbnail': 'abi-scaffold-thumbnail',
        'image/png': _COMMON_IMAGES,
        'image/tiff': 'tiff-image',
        'image/tif': 'tiff-image',
        'image/jpeg': _COMMON_IMAGES,
        'image/jpx': 'large-3d-image',
        'image/jp2': 'large-2d-image',
        'video/mp4': 'mp4'
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
        'audio/midi',
        'text/x-sh',
        'image/vnd.zeiss.czi',
        'image/vnd.nikon.nd2',
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
    if mime_type == _NOT_SPECIFIED:
        return _SKIP

    if mime_type in skipped_mime_types:
        return _SKIP

    if mime_type in mapped_mime_types:
        if mime_type in ["image/jpeg", "image/png"]:
            # print('=====================')
            # print('jpeg obj: ', obj)
            try:
                # print(obj['dataset']['path'])
                if obj['dataset']['path'].startswith('derivative'):
                    return _SKIP
            except KeyError:
                return _SKIP
            # print('=====================')
        return mapped_mime_types[mime_type]

    return _NOT_SPECIFIED


def _sort_files_by_mime_type(obj_list):
    sorted_files = {}
    if not obj_list:
        return sorted_files

    for obj in obj_list:
        mime_type = obj.get('mimetype', _NOT_SPECIFIED)
        # print("object:", mime_type)
        mapped_mime_type = _mapped_mime_type(mime_type, obj)
        if mapped_mime_type == _NOT_SPECIFIED:
            logging.warning('Unhandled mime type:', mime_type)
        elif mapped_mime_type == _SKIP:
            pass
        else:
            if mapped_mime_type in sorted_files:
                sorted_files[mapped_mime_type].append(obj)
            else:
                sorted_files[mapped_mime_type] = [obj]

    return sorted_files


# get_attributes: Use 'attributes' (defined at top of this document) to step through the large sci-crunch result dict
#  and cherry-pick the attributes of interest
def _get_attributes(attributes_, dataset):
    found_attr = {}
    for k, attr in attributes_.items():
        subset = dataset['_source']  # set our subset to the full dataset result
        key_attr = False
        for key in attr:
            if isinstance(subset, dict):
                if key in subset.keys():
                    subset = subset[key]
                    key_attr = subset
        found_attr[k] = key_attr
    return found_attr


# Manipulate the output to make it easier to use in the front-end.
# Tasks performed:
# - collate the scaffold data information onto the scaffolds list.
def _manipulate_attr(output):
    if 'scaffolds' in output and 'abi-scaffold-file' in output:
        for scaffold in output['scaffolds']:
            id_ = scaffold['dataset']['id']
            scaffold_meta_file = _extract_dataset_path_remote_id(output, 'abi-scaffold-file', id_)

            scaffold_thumbnail = None
            if 'abi-scaffold-thumbnail' in output:
                scaffold_thumbnail = _extract_dataset_path_remote_id(output, 'abi-scaffold-thumbnail', id_)

            if scaffold_meta_file is not None:
                scaffold['meta_file'] = scaffold_meta_file
            if scaffold_thumbnail is not None:
                scaffold['thumbnail'] = scaffold_thumbnail

    return output


def _extract_dataset_path_remote_id(data, key, id_):
    extracted_data = None
    for dataset_path_remote_id in data[key]:
        if dataset_path_remote_id['dataset']['id'] == id_:
            remote_id = ''
            if 'remote' in dataset_path_remote_id:
                remote_id = dataset_path_remote_id['remote']['id']
            extracted_data = {
                'path': dataset_path_remote_id['dataset']['path'],
                'remote_id': remote_id
            }
            break

    return extracted_data
