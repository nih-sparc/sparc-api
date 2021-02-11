import json

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
    'csvFiles': ['objects']
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
                "must": {},
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
        data['query']['bool']['must'] = {
            "query_string": {
                query_options
            }
        }
    return data


# process_kb_results: Loop through sci-crunch results pulling out desired attributes and processing DOIs and CSV files
def process_kb_results(results):
    output = []
    hits = results['hits']['hits']
    for i, hit in enumerate(hits):
        attr = get_attributes(attributes, hit)
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


# get_attributes: Use 'attributes' (defined at top of this document) to step through the large sci-crunch result dict
#  and cherry-pick the attributes of interest
def get_attributes(attributes_, dataset):
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
