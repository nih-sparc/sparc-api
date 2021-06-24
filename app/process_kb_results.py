import json

# attributes is used to map desired parameters onto the path of keys needed in the scicrunch response.
#  For example:
#  samples: ['attributes','sample','subject'] will find and enter dict keys in the following order:
#  attributes > sample > subject
attributes = {
    'additionalLinks': ['xrefs', 'additionalLinks'],
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

# create_facet_query(type): Generates facet search request data for scicrunch  given a 'type'; where
# 'type' is either 'species', 'gender', or 'organ' at this stage.
#  Returns a tuple of the typemap and request data ( type_map, data )
def create_facet_query(type):
    type_map = {
        'species': ['organisms.primary.species.name.aggregate', 'organisms.sample.species.name.aggregate'],
        'gender': ['attributes.subject.sex.value'],
        'organ': ['anatomy.organ.name.aggregate']
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

    if not query and not terms and not facets:
        return {"size": size, "from": start}

    # Type map is used to map scicrunch paths to given facet
    type_map = {
        'species': ['organisms.primary.species.name.aggregate', 'organisms.sample.species.name'],
        'gender': ['attributes.subject.sex.value', 'attributes.sample.sex.value'],
        'organ': ['anatomy.organ.name.aggregate']
    }

    # Data structure of a scicrunch search
    qs = facet_query_string(query, terms, facets, type_map)

    if qs:
        return {
            "size": size,
            "from": start,
            "query": {
                "query_string": {
                    "query": qs
                }
            }
        }

    return {
        "size": size,
        "from": start,
    }


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
            if term not in t.keys():  # If term hasn't been seen, add it to the list of terms
                t[term] = [facets[i]]
            else:
                t[term].append(facets[i])  # If term has been seen append it to it's term

    # Add search query if it exists
    qt = ""
    if query:
        qt = f'({query})'

    if query and t:
        qt += " AND "

    # Add the brackets and OR and AND parameters
    for k in t:
        if k == "datasets":
            needParentheses = (qt or len(t) > 1) and (len(t[k]) > 1)
            if needParentheses:
                qt += "("
            for l in t[k]:
                if l == "scaffolds":
                    qt += "objects.additional_mimetype.name:((inode%2fvnd.abi.scaffold) AND (file))"
                elif l == "simulations":
                    qt += "xrefs.additionalLinks.description:((CellML) OR (SED-ML))"
                if l is not t[k][-1]:
                    qt += " OR "  # 'OR' if more terms in this facet are coming
            if needParentheses:
                qt += ")"
        else:
            qt += type_map[k][0] + ":("  # facet term path and opening bracket
            for l in t[k]:
                qt += f"({l})"  # bracket around terms incase there are spaces
                if l is not t[k][-1]:
                    qt += " OR "  # 'OR' if more terms in this facet are coming
                else:
                    qt += ")"

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
        objects = attr['csvFiles']  # Have to do this as not all datsets return objects
        attr['csvFiles'] = find_csv_files(objects)
        attr['scaffolds'] = find_scaffold_json_files(objects)
        output.append(attr)
    return json.dumps({'numberOfHits': results['hits']['total'], 'results': output})


def convert_doi_to_url(doi):
    if not doi:
        return doi
    return doi.replace('DOI:', 'https://doi.org/')

def convert_url_to_doi(doi):
    if not doi:
        return doi
    return doi.replace('https://doi.org/', 'DOI:')


def find_csv_files(obj_list):
    if not obj_list:
        return obj_list
    return [obj for obj in obj_list if obj.get('mimetype', {}).get('name', 'none') == 'text/csv']


def find_scaffold_json_files(obj_list):
    if not obj_list:
        return obj_list
    return [obj for obj in obj_list if obj.get('additional_mimetype', {}).get('name', 'none') == 'inode/vnd.abi.scaffold+file']


# get_attributes: Use 'attributes' (defined at top of this document) to step through the large scicrunch result dict
#  and cherrypick the attributes of interest
def get_attributes(attributes, dataset):
    found_attr = {}
    for k, attr in attributes.items():
        subset = dataset['_source'] # set our subest to the full dataset result
        key_attr = False
        for n, key in enumerate(attr): # step through attributes
            if isinstance(subset, dict):
                if key in subset.keys(): # continue if keys are found
                    subset = subset[key]
                    if n+1 is len(attr): # if we made it to the end, save this subset
                        key_attr = subset
        found_attr[k] = key_attr
    return found_attr

