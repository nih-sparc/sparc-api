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
        "match": {
            "item.curie": {
                "query": f"doi:{doi}",
                "operator": "and"
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
#  Returns a tuple of the type-map and request data ( type_map, data )
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


# create_facet_query(query, terms, facets, size, start): Generates filter search request data for SciCrunch
#  All inputs to facet query have defaults defined as 'None' (this is done so we can directly take in URL params
#  as input).
#  Returns a json query to be used in a SciCrunch request as request json data
def create_filter_request(query, terms, facets, size, start):
    if size is None:
        size = 10
    if start is None:
        start = 0

    if query == "" and len(terms) == 0 and len(facets) == 0:
        return {"size": size, "from": start}

    # Type map is used to map SciCrunch paths to given facet
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
        for ll in t[k]:
            qt += f"({ll})"  # bracket around terms in case there are spaces
            if ll is not t[k][-1]:
                qt += " OR "  # 'OR' if more terms in this facet are coming
            else:
                qt += ") "

        if k is not list(t.keys())[-1]:  # Add 'AND' if we are not at the last item
            qt += " AND "
    return qt
