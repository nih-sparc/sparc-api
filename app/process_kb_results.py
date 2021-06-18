import json
import logging

from app.config import Config

_NOT_SPECIFIED = 'not-specified'
_SKIP = 'skip'
_COMMON_IMAGES = 'common-images'
_SEGMENTATION_FILES = 'mbf-segmentation'

_PASS_THROUGH_KEYS = [_COMMON_IMAGES, _SEGMENTATION_FILES]

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

def create_doi_request(doi):

    query = {
        "query": {
            "bool": {
                "must": [{"match_all": {}}],
                "should": [],
                "filter": {
                    "term": {
                        "_id": doi
                    }
                }
            }
        }
    }

    return query

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
    output = dict(filter(lambda x: x[0] in _PASS_THROUGH_KEYS, result.items()))
    if _COMMON_IMAGES in result:
        output[_COMMON_IMAGES] = []
        for common_image in result[_COMMON_IMAGES]:
            if 'bytes' in common_image and int(common_image['bytes']['count']) < Config.DIRECT_DOWNLOAD_LIMIT:
                output[_COMMON_IMAGES].append(common_image)

    return output


def reform_dataset_results(results):
    processed_outputs = []
    kb_results = _prepare_kb_results(results)
    for kb_result in kb_results:
        processed_outputs.append(_process_kb_result(kb_result))

    return {'result': processed_outputs}


def _convert_doi_to_url(doi):
    if not doi:
        return doi
    return doi.replace('DOI:', 'https://doi.org/')


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
    if obj['remote']['id'] in _NEUROLUCIDA_FUDGES:
        obj['mimetype'] = 'application/vnd.mbfbioscience.neurolucida+xml'
    if obj.get('mimetype', 'none') == 'application/json' and "metadata.json" in obj.get('dataset', 'none')['path']:
        obj['mimetype'] = 'inode/vnd.abi.scaffold+file'

    return obj


def _mapped_mime_type(mime_type, obj):
    mapped_mime_types = {
        'text/csv': 'csv',
        'application/vnd.mbfbioscience.metadata+xml': _SEGMENTATION_FILES,
        'application/vnd.mbfbioscience.neurolucida+xml': _SEGMENTATION_FILES,
        'inode/vnd.abi.scaffold+directory': 'abi-scaffold-dir',
        'inode/vnd.abi.scaffold+file': 'abi-scaffold-metadata-file',
        'inode/vnd.abi.scaffold+thumbnail': 'abi-scaffold-thumbnail',
        'text/vnd.abi.plot+Tab-separated-values': 'abi-plot',
        'text/vnd.abi.plot+csv': 'abi-plot',
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
            try:
                if obj['dataset']['path'].startswith('derivative'):
                    return _SKIP
            except KeyError:
                return _SKIP
        return mapped_mime_types[mime_type]

    return _NOT_SPECIFIED


def _sort_files_by_mime_type(obj_list):
    sorted_files = {}
    if not obj_list:
        return sorted_files

    for obj in obj_list:
        # Hacks are applied here.
        obj = _fudge_object(obj)

        mime_type = obj.get('mimetype', _NOT_SPECIFIED)

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
