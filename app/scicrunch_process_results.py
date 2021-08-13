import json
import importlib

# from app.utilities import print_hit_structure


# process_kb_results: Loop through SciCrunch results pulling out desired attributes and processing DOIs and CSV files
def _prepare_results(results):
    output = []
    hits = results['hits']['hits']
    for i, hit in enumerate(hits):
        try:
            version = hit['_source']['item']['version']['keyword']
        except KeyError:
            continue

        package_version = f'scicrunch_processing_v_{version.replace(".", "_")}'
        m = importlib.import_module(f'app.{package_version}')
        attributes_map = getattr(m, 'ATTRIBUTES_MAP')
        sort_files_by_mime_type = getattr(m, 'sort_files_by_mime_type')
        # print_hit_structure(hit)
        attr = _transform_attributes(attributes_map, hit)
        attr['doi'] = _convert_doi_to_url(attr['doi'])
        attr['took'] = results['took']
        attr.update(sort_files_by_mime_type(attr['files']))

        output.append(attr)

    return output


def process_results(results):
    return json.dumps({'numberOfHits': results['hits']['total'], 'results': _prepare_results(results)})


def reform_dataset_results(results):
    processed_outputs = []
    kb_results = _prepare_results(results)
    for kb_result in kb_results:
        version = kb_result['version']
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
        for key in attr:
            if isinstance(subset, dict):
                if key in subset.keys():
                    subset = subset[key]
                    if attr[-1] == key:
                        key_attr = subset
        found_attr[k] = key_attr
    return found_attr
