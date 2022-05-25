def create_query_string(query_string):
    return {
        "from": 0,
        "size": 20,
        "query": {
            "query_string": {
                "query": f"{query_string}"
            }
        }
    }


def create_doi_query(doi):
    return {
        "query": {
            "term": {
                "item.curie": doi
            }
        }
    }

def create_multiple_doi_query(dois):
    return {
        "size": 999,
        "query": {
            "terms": {
                "item.curie": dois
            }
        }
    }

def create_multiple_discoverId_query(ids):
    return {
        "size": 999,
        "query": {
            "terms": {
                "pennsieve.identifier": ids
            }
        }
    }


def create_title_query(title):
    parts = title.split(' ')
    alphanum_parts = []
    for p in parts:
        alphanum_parts.append(''.join(e for e in p if e.isalnum()))

    query = ['(' + p + ')' for p in alphanum_parts if p]
    return {
        "size": 10,
        "from": 0,
        "query": {
            "query_string": {
                "fields": [
                    "item.name"
                ],
                "query": " AND ".join(query)
            }
        }
    }


def create_identifier_query(identifier):
    parts = identifier.split(':')
    query = f'*{parts[1]}'

    return {
        "size": 10,
        "from": 0,
        "query": {
            "query_string": {
                "fields": [
                    "*identifier"
                ],
                "query": query
            }
        }
    }


def create_pennsieve_identifier_query(identifier):
    return {
        "query": {
            "term": {
                "pennsieve.identifier.aggregate": identifier
            }
        }
    }


def create_field_query(field, search_term, size=10, from_=0):
    return {
        "size": size,
        "from": from_,
        "query": {
            "query_string": {
                "fields": [
                    field
                ],
                "query": search_term
            }
        }
    }


def create_onto_term_query(term, existing_id_type='iri'):
    return {
        "size": 10,
        "from": 0,
        "query": {
            "bool": {
                "must": [{
                    "match_phrase": {
                        f"existing_ids.{existing_id_type}": {
                            "query": term
                        }
                    }
                }]
            }
        }
    }


def create_doi_aggregate(size=1000):
    return {
        "from": 0,
        "size": 0,
        "aggregations": {
            "doi": {
                "composite": {
                    "size": size,
                    "sources": [
                        {
                            "curie": {"terms": {"field": "item.curie.aggregate"}}
                        }
                    ],
                    "after": {"curie": ""}
                }
            }
        }
    }


def create_doi_request(doi):
    query = {
        "query": {
            "bool": {
                "must": [{"match_all": {}}],
                "should": [],
                "filter": {
                    "item": {
                        "curie": doi
                    }
                }
            }
        }
    }

    return query


# create_facet_query(type): Generates facet search request data for sci-crunch  given a 'type'; where
# 'type' is either 'species', 'gender', or 'organ' at this stage.
#  Returns a tuple of the type-map and request data ( type_map, data )
def create_facet_query(type_):
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

    return get_facet_type_map(), data


# create_facet_query(query, terms, facets, size, start): Generates filter search request data for SciCrunch
#  All inputs to facet query have defaults defined as 'None' (this is done so we can directly take in URL params
#  as input).
#  Returns a json query to be used in a SciCrunch request as request json data
def create_filter_request(query, terms, facets, size, start):
    if size is None:
        size = 10
    if start is None:
        start = 0

    if not query and not terms and not facets:
        return {"size": size, "from": start}

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

    qs = facet_query_string(query, terms, facets, get_facet_type_map())
    data["query"]["query_string"]["query"] = qs

    return data


# Type map is used to map SciCrunch paths to given facet
# genotype is deprecated.
def get_facet_type_map():
    return {
        'species': ['organisms.primary.species.name.aggregate', 'organisms.sample.species.name.aggregate', 'organisms.scaffold.species.name.aggregate'],
        'Species': ['organisms.primary.species.name.aggregate', 'organisms.sample.species.name.aggregate',
                    'organisms.scaffold.species.name.aggregate'],
        'gender': ['attributes.subject.sex.value'],
        'Gender': ['attributes.subject.sex.value'],
        'sex': ['attributes.subject.sex.value'],
        'Sex': ['attributes.subject.sex.value'],
        'genotype': ['anatomy.organ.name.aggregate'],
        'Anatomical structure': ['anatomy.organ.name.aggregate'],
        'organ': ['anatomy.organ.name.aggregate'],
        'Organ': ['anatomy.organ.name.aggregate'],
        'Experimental approach': ['item.modalities.keyword'],
        'Age categories': ['attributes.subject.ageCategory.value']
    }


def facet_query_string(query, terms, facets, type_map):
    # We will create AND OR structure. OR within facets and AND between them
    # Example Output:
    #
    # "heart AND attributes.subject.sex.value:((male) OR (female))"

    t = {}
    for i, term in enumerate(terms):
        if (term is None or facets[i] is None or 'show' in facets[i].lower() or 'all' in facets[i].lower()):  # Ignore 'Show all' facets
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
        if k == "datasets":
            needParentheses = (qt or len(t) > 1) and (len(t[k]) > 1)
            if needParentheses:
                qt += "("
            for l in t[k]:
                if l == "scaffolds":
                    qt += "objects.additional_mimetype.name:((inode%2fvnd.abi.scaffold) AND (file))"
                if l is not t[k][-1]:
                    qt += " OR "  # 'OR' if more terms in this facet are coming
            if needParentheses:
                qt += ")"
        else:
            qt += "("
            for m in type_map[k]:
                qt += m + ":("  # facet term path and opening bracket
                for l in t[k]:
                    qt += f"({l})"  # bracket around terms incase there are spaces
                    if l is not t[k][-1]:
                        qt += " OR "  # 'OR' if more terms in this facet are coming
                    else:
                        qt += ")"
                if m is not type_map[k][-1]:
                    qt += " OR "
            qt += ")"
        if k is not list(t.keys())[-1]:  # Add 'AND' if we are not at the last item
            qt += " AND "
    return qt


# create the request body for requesting list of uberon ids
def create_request_body_for_curies(species):
    body = {
        "from": 0,
        "size": 0,
        "aggregations": {
            "names_and_curies": {
                "terms": {
                    "script": {
                        "lang": "painless",
                        "inline": "def a=null;if(params['_source']['anatomy'] != null ) { if(params['_source']['anatomy']['organ'] != null ) { a = params['_source']['anatomy']['organ'];}} return a;"
                    },
                    "size": 200
                }
            }
        }
    }

    # Construct the query if there is a list of species 
    if len(species) > 0:
        query = {
            "query_string": {
                "fields": [
                    "*species.name"
                ],
            }
        }

        query_string = ''

        for item in species:
            if item != species[0]:
                query_string += ' OR '
            query_string += f"({item})"
        query["query_string"]["query"] = query_string
        body['query'] = query

    return body
