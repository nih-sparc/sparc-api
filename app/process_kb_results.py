import json

def process_kb_results_recursive(results):
    output = []
    hits = results['hits']['hits']
    for i, hit in enumerate(hits):
        output.append({})
        process_dict_recursive(hit['_source'], output[i])
    print(output)
    return json.dumps({'numberOfHits': results['hits']['total'], 'results': output})

from collections.abc import Mapping
def process_dict_recursive(leveled_dict, output, keyList=[]):
    if isinstance(leveled_dict, Mapping):
        for key in leveled_dict:
            process_dict_recursive(leveled_dict[key], output, key)
    else:
        output[keyList] = leveled_dict

# This function flattens a dictionary to one level with the level accessed by lookup keys that travel through the
# nested dict.
# (currently unused)
def process_dict_recursive_with_lookups(leveled_dict, output, keyList=[], depth=0):
    if isinstance(leveled_dict, Mapping):
        keyList.append('')
        depth += 1
        for key in leveled_dict:
            keyList[depth - 1] = key
            process_dict_recursive_with_lookups(leveled_dict[key], output, keyList, depth)
    else:
        lookup = ''
        for val in keyList:
            lookup += val
            lookup += '.'
        output[lookup] = leveled_dict

