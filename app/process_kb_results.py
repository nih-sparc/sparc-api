import json

# attributes is used to map desired parameters onto the path of keys needed in the scicrunch response.
#  For example:
#  samples: ['attributes','sample','subject'] will find and enter dict keys in the following order:
#  attributes > sample > subject
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


# create_facet_query(type): Generates facet search request data for scicrunch  given a 'type'; where
# 'type' is either 'species', 'gender', or 'genotype' at this stage.
#  Returns a tuple of the typemap and request data ( type_map, data )
def create_facet_query(type):
    type_map = {
        'species': ['organisms.primary.species.name.aggregate', 'organisms.sample.species.name.aggregate'],
        'gender': ['attributes.subject.sex.value'],
        'genotype': ['anatomy.organ.name.aggregate']
    }

    data = {
        "from": 0,
        "size": 0,
        "aggregations": {
            f"{type}": {
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


# create_facet_query(query, terms, facets, size, start): Generates filter search request data for scicrunch
#  All inputs to facet query have defaults defined as 'None' (this is done so we can directly take in URL params
#  as input).
#  Returns a json query to be used in a scicrunch request as request json data
def create_filter_request(query, terms, facets, size, start):
    if size is None:
        size = 10
    if start is None:
        start = 0

    # Type map is used to map scicrunch paths to given facet
    type_map = {
        'species': ['organisms.primary.species.name.aggregate', 'organisms.sample.species.name'],
        'gender': ['attributes.subject.sex.value', 'attributes.sample.sex.value'],
        'genotype': ['anatomy.organ.name.aggregate']
    }

    # Data structure of a scicrunch search
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

    print(data)
    return data


def facet_query_string(query, terms, facets, type_map):

    # We will create AND OR structure. OR within facets and AND between them
    # Example Output:
    #
    # "heart AND attributes.subject.sex.value:((male) OR (female))"

    t = {}
    for i, term in enumerate(terms):
        if (term is None or facets[i] is None or 'All' in facets[i]):  # Ignore 'All species' facets
            continue
        else:
            if term not in t.keys():  # No duplicate terms for the typemap. OR will be handled later
                t[term] = [facets[i]]
            else:
                t[term].append(facets[i])

    # Add search query if it exists
    qt = ""
    if query is not "":
        qt = f'({query}) AND '

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


# process_kb_results: Loop through scicrunch results pulling out desired attributes and processing doi's and csv files
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


# get_attributes: Use 'attributes' (defined at top of this document) to step through the large scicrunch result dict
#  and cherrypick the attributes of interest
def get_attributes(attributes, dataset):
    found_attr = {}
    for k, attr in attributes.items():
        subset = dataset['_source'] # set our subest to the full dataset result
        key_attr = False
        for key in attr:
            if isinstance(subset, dict):
                if key in subset.keys():
                    subset = subset[key]
                    key_attr = subset
        found_attr[k] = key_attr
    return found_attr

