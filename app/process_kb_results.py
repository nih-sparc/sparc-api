import json
from collections.abc import Mapping

attributes = {
    'scaffolds': ['scaffold'],
    'samples': ['attributes','sample','subject'],
    'name': ['item','name'],
    'identifier': ['item', 'identifier'],
    'uri': ['distributions', 'current', 'uri'],
    'updated': ['dates', 'updated'],
    'organs': ['anatomy', 'organ'],
    'contributors': ['contributors']
}

def process_kb_results_recursive(results):
    output = []
    hits = results['hits']['hits']
    for i, hit in enumerate(hits):
        attr = getAttributes(attributes, hit)
        output.append(attr)
    return json.dumps({'numberOfHits': results['hits']['total'], 'results': output})


# This function flattens a nested dictionary to dictionary with depth of 1
def flatten_dict_recursive(leveled_dict, output, keyList=[]):
    if isinstance(leveled_dict, Mapping):
        for key in leveled_dict:
            flatten_dict_recursive(leveled_dict[key], output, key)
    else:
        output[keyList] = leveled_dict


def getScaffolds(dataset):
    scaffolds = []
    if 'scaffolds' in dataset['_source'].keys():
        for scaffold in dataset['_source']['scaffolds']:
            scaffolds.append(scaffold)
    return scaffolds

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

# This function flattens a nested dictionary and preserves lookup keys
# (currently unused)
def flatten_dict_recursive_with_lookups(leveled_dict, output, keyList=[], depth=0):
    if isinstance(leveled_dict, Mapping):
        keyList.append('')
        depth += 1
        for key in leveled_dict:
            keyList[depth - 1] = key
            flatten_dict_recursive_with_lookups(leveled_dict[key], output, keyList, depth)
    else:
        lookup = ''
        for val in keyList:
            lookup += val
            lookup += '.'
        output[lookup] = leveled_dict

