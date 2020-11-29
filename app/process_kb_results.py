import json

attributes = {
    'scaffolds': ['scaffolds'],
    'samples': ['attributes','sample','subject'],
    'name': ['item','name'],
    'identifier': ['item', 'identifier'],
    'uri': ['distributions', 'current', 'uri'],
    'updated': ['dates', 'updated'],
    'organs': ['anatomy', 'organ'],
    'contributors': ['contributors'],
    'doi': ['item', 'curie'],
    'csvFiles': ['objects']
}

def process_kb_results(results):
    output = []
    hits = results['hits']['hits']
    for i, hit in enumerate(hits):
        attr = getAttributes(attributes, hit)
        attr['doi'] = convert_doi_to_url(attr['doi'])
        attr['csvFiles'] = find_csv_files(attr['csvFiles'])
        output.append(attr)
    return json.dumps({'numberOfHits': results['hits']['total'], 'results': output})

def convert_doi_to_url(doi):
    if not doi:
        return doi
    return doi.replace('DOI:', 'https://doi.org/')

def find_csv_files(obj_list):
    if not obj_list:
        return obj_list
    return [obj for obj in obj_list if obj.get('mimetype', 'none') == 'text/csv']

def getAttributes(attributes, dataset):
    found_attr = {}
    for k, attr in attributes.items():
        subset = dataset['_source']
        key_attr = False
        for key in attr:
            if isinstance(subset, dict):
                if key in subset.keys():
                    subset = subset[key]
                    key_attr = subset
        found_attr[k] = key_attr
    return found_attr

