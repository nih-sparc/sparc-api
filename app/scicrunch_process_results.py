import importlib
import json
import re
from app.config import Config
from flask import jsonify

from app.scicrunch_processing_common import SKIPPED_OBJ_ATTRIBUTES


# process_kb_results: Loop through SciCrunch results pulling out desired attributes and processing DOIs and CSV files
def _prepare_results(results):
    output = []
    hits = results['hits']['hits']
    for i, hit in enumerate(hits):
        try:
            version = hit['_source']['item']['version']['keyword']
        except KeyError:
            # Try to get minimal information out from the datasets
            version = 'undefined'

        package_version = f'scicrunch_processing_v_{version.replace(".", "_")}'
        m = importlib.import_module(f'app.{package_version}')
        attributes_map = getattr(m, 'ATTRIBUTES_MAP')
        sort_files_by_mime_type = getattr(m, 'sort_files_by_mime_type')
        # print_hit_structure(hit)
        attr = _transform_attributes(attributes_map, hit)
        attr['doi'] = _convert_doi_to_url(attr['doi'])
        attr['took'] = results['took']

        # Hot fix for some datasets having no objects:
        if 'objects' in hit['_source'].keys():
            # Find context files by looking through object mimetypes.
            attr['abi-contextual-information'] = [
                file['dataset']['path']
                for file in hit['_source']['objects']
                if 'additional_mimetype' in file and \
                   file['additional_mimetype']['name'].find('abi.context-information') != -1
            ]
        else:
            attr['abi-contextual-information'] = []

        try:
            attr['readme'] = hit['_source']['item']['readme']['description']
        except KeyError:
            attr['readme'] = ''

        try:
            attr['title'] = hit['_source']['item']['name']
        except KeyError:
            attr['title'] = ''

        _remove_unused_files_information(attr['files'])
        attr.update(sort_files_by_mime_type(attr['files']))
        # All files are sorted, files are not required anymore
        del attr['files']
        output.append(attr)

    return output


# Remove unused attributes in the obj list, this does not need to be version dependent at this moment
def _remove_unused_files_information(obj_list):
    if not obj_list:
        return None

    for obj in obj_list:
        for key in SKIPPED_OBJ_ATTRIBUTES:
            if key in obj:
                del obj[key]


def process_results(results):
    return jsonify({'numberOfHits': results['hits']['total'], 'results': _prepare_results(results)})

# process the search result to get the first scaffold of the first dataset
def process_get_first_scaffold_info(results):
    results = _prepare_results(results)
    # iterate through to get the first scaffold
    for result in results:
        if 'abi-scaffold-metadata-file' in result and len(result['abi-scaffold-metadata-file']) > 0:
            try:
                path = result['abi-scaffold-metadata-file'][0]['dataset']['path']
                id = result['dataset_identifier']
                version = result['dataset_version']
                s3uri = result['s3uri']
                return jsonify({'path':path, 'id': id, 'version': version, 's3uri': s3uri})
            except KeyError:
                return None

    #None found, let the caller handle that
    return None

def reform_anatomy_results(results):
    processed_outputs = []
    hits = results['hits']['hits']
    for i, hit in enumerate(hits):
        processed_outputs.append(hit['_source'])

    return {'result': processed_outputs}


def reform_dataset_results(results):
    processed_outputs = []
    kb_results = _prepare_results(results)
    for kb_result in kb_results:
        try:
            version = kb_result['version']
        except KeyError:
            # Try to get minimal information out from the datasets
            version = 'undefined'
        package_version = f'scicrunch_processing_v_{version.replace(".", "_")}'
        m = importlib.import_module(f'app.{package_version}')
        process_result = getattr(m, 'process_result')
        processed_outputs.append(process_result(kb_result))

    return {'result': processed_outputs}


def reform_aggregation_results(results):
    processed_results = []
    if 'aggregations' in results:
        processed_results = results['aggregations']

    return processed_results


def _convert_doi_to_url(doi):
    if not doi:
        return doi
    return doi.replace('DOI:', 'https://doi.org/')


# _transform_attributes: Use 'attributes' (defined per version) to step through the large sci-crunch result dict
#  and cherry-pick the attributes of interest
def _transform_attributes(attributes_, dataset):
    found_attr = {}
    for k, attr in attributes_.items():
        subset = dataset['_source']  # set our subset to the full dataset result
        key_attr = False
        for n, key in enumerate(attr):
            if isinstance(subset, dict):
                if key in subset.keys():  # continue if keys are found
                    subset = subset[key]
                    if n + 1 is len(attr):  # if we made it to the end, save this subset
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
            if 'identifier' in dataset_path_remote_id:
                remote_id = dataset_path_remote_id['identifier']
            # if 'remote' in dataset_path_remote_id:
            #     remote_id = dataset_path_remote_id['remote']['id']
            extracted_data = {
                'path': dataset_path_remote_id['dataset']['path'],
                'remote_id': remote_id
            }
            break

    return extracted_data


# Turn the result into a list in the uberon.array field
def reform_curies_results(data):
    result = {
        'uberon': {
            'array': []
        }
    }
    id_name_map = {}
    # Iterate through to get an uberon - name map
    for item in data['aggregations']['names_and_curies']["buckets"]:
        try:
            # The key object is returned as a string - use re.search to extract
            # Example string: 
            # "{curie=UBERON:0002298, name=brainstem, matchingStatus=Exact Match}"
            pattern = "curie=(.*?),"
            curie = ''
            match = re.search(pattern, item['key'])
            if match:
                curie = match.group(1)

            pattern = "name=(.*?),"
            name = ''
            match = re.search(pattern, item['key'])
            if match:
                name = match.group(1)

            if curie and name:
                id_name_map[curie] = name
        except KeyError:
            continue
    # Turn the map into an the output array
    for key in id_name_map:
        pair = {
            'id': key,
            'name': id_name_map[key]
        }
        result['uberon']['array'].append(pair)

    return result


# Turn the result into a list in the uberon.array field
def reform_related_terms(data):
    result = {
        'uberon': {
            'array': []
        }
    }
    id_name_map = {}

    # Iterate through to get an uberon - name map
    if 'nodes' in data:
        for item in data['nodes']:
            id_name_map[item['id']] = item['lbl']
    else:
        raise BaseException

    if 'edges' in data:
        for item in data['edges']:
            pair = {
                'id': item['obj'],
                'name': id_name_map[item['obj']]
            }
            result['uberon']['array'].append(pair)
    else:
        raise BaseException

    return result
