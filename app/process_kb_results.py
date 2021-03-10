import json
import logging

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

    # Type map is used to map sci-crunch paths to given facet
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
            "bool": {
                "must": [],
                "should": [],
                "filter": []
            }
        }
    }

    # Add a filter for each facet
    for i, facet in enumerate(facets):
        if terms[i] is not None and facet is not None and 'All' not in facet:
            data['query']['bool']['filter'].append({'term': {f'{type_map[terms[i]][0]}': f'{facet}'}})

    # Add queries if they exist
    if query:
        query_options = {
            "query": f"{query}",
            "default_operator": "and",
            "lenient": "true",
            "type": "best_fields"
        }
        data['query']['bool']['must'].append({
            "query_string": query_options
        })
    return data


# process_kb_results: Loop through sci-crunch results pulling out desired attributes and processing DOIs and CSV files
def process_kb_results(results):
    output = []
    hits = results['hits']['hits']
    for i, hit in enumerate(hits):
        attr = _get_attributes(attributes, hit)
        attr['doi'] = _convert_doi_to_url(attr['doi'])
        attr.update(_sort_files_by_mime_type(attr['files']))

        output.append(_manipulate_attr(attr))

    return json.dumps({'numberOfHits': results['hits']['total'], 'results': output})


def _convert_doi_to_url(doi):
    if not doi:
        return doi
    return doi.replace('DOI:', 'https://doi.org/')


def _sort_files_by_mime_type(obj_list):
    sorted_files = {}
    if not obj_list:
        return sorted_files

    mapped_mime_types = {
        'text/csv': 'csv',
        'application/vnd.mbfbioscience.metadata+xml': 'mbf-segmentation',
        'application/vnd.mbfbioscience.neurolucida+xml': 'mbf-segmentation',
        'inode/vnd.abi.scaffold+directory': 'abi-scaffold-dir',
        'inode/vnd.abi.scaffold+file': 'abi-scaffold-file',
        'inode/vnd.abi.scaffold+thumbnail': 'abi-scaffold-thumbnail',
        'image/png': 'generic-image',
        'image/tiff': 'generic-image',
        'image/tif': 'generic-image',
        'image/jpeg': 'generic-image',
        'image/jpx': 'large-3d-image',
        'image/jp2': 'large-2d-image',
        'video/mp4': 'mp4'
    }
    skipped_mime_types = [
        'application/json',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        'chemical/x-gamess-input',
        'application/pdf',
        'application/zip',
        'application/xml',
        'inode/directory',
        'application/vnd.oasis.opendocument.database',
        'text/plain',
        'text/html',
        'text/css',
    ]

    for obj in obj_list:
        mime_type = obj.get('mimetype', 'not-specified')
        if mime_type in mapped_mime_types:
            if mapped_mime_types[mime_type] in sorted_files:
                sorted_files[mapped_mime_types[mime_type]].append(obj)
            else:
                sorted_files[mapped_mime_types[mime_type]] = [obj]
        elif mime_type == 'not-specified':
            pass
        elif mime_type in skipped_mime_types:
            pass
        else:
            logging.warning('Unhandled mime type:', mime_type)

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
