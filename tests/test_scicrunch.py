import json
import pytest
from packaging import version

from app import app
from app.main import dataset_search
from app.scicrunch_requests import create_query_string

from known_uberons import UBERONS_DICT
from known_dois import has_doi_changed, warn_doi_changes


@pytest.fixture
def client():
    # Spin up test flask app
    app.config['TESTING'] = True
    return app.test_client()


def test_scicrunch_keys(client):
    r = client.get('/search/')
    assert r.status_code == 200
    assert 'numberOfHits' in json.loads(r.data).keys()


def check_doi_status(client, dataset_id, doi):
    r = client.get('/dataset_info/using_pennsieve_identifier', query_string={'identifier': dataset_id})
    response = json.loads(r.data)
    result = response['result'][0]
    status = True
    if version.parse(result['version']) >= version.parse("1.1.4"):
        if has_doi_changed(result['doi'].replace('https://doi.org/', ''), doi):
            warn_doi_changes()
            status = False

    return status


def test_scicrunch_dataset_doi(client):
    # Testing with dataset 55
    identifier = "55"
    run_doi_test = check_doi_status(client, identifier, '10.26275/pzek-91wx')

    if run_doi_test:
        r = client.get('/scicrunch-dataset/DOI%3A10.26275%2Fpzek-91wx')
        dataset_version = json.loads(r.data)['hits']['hits'][0]['_source']['item']['version']['keyword']
        if version.parse(dataset_version) >= version.parse("1.1.4"):
            assert json.loads(r.data)['hits']['hits'][0]['_id'] == "55"
            assert json.loads(r.data)['hits']['hits'][0]['_source']['item']['curie'] == "DOI:10.26275/pzek-91wx"
        else:
            assert json.loads(r.data)['hits']['hits'][0]['_id'] == "DOI:10.26275/pzek-91wx"
    else:
        pytest.skip('DOI used in test is out of date.')

def test_scicrunch_multiple_dataset_doi(client):
    # Testing with dataset 55 and 68
    run_doi_test_1 = check_doi_status(client, "55", '10.26275/pzek-91wx')
    run_doi_test_2 = check_doi_status(client, "68", '10.26275/4qvr-kwzq')

    if run_doi_test_1 and run_doi_test_2:
        r = client.get('/dataset_info/using_multiple_dois/?dois=10.26275%2Fpzek-91wx&dois=10.26275%2F4qvr-kwzq')
        results = json.loads(r.data)['results']
        dataset_version = results[0]['version']
        if version.parse(dataset_version) >= version.parse("1.1.4"):
            discover_id_1 = results[0]['dataset_identifier']
            discover_id_2 = results[1]['dataset_identifier']
            assert discover_id_1 == "55" or discover_id_1 == "68"
            assert discover_id_2 == "55" or discover_id_2 == "68"
    else:
        pytest.skip('DOI used in test is out of date.')

def test_scicrunch_multiple_dataset_ids(client):
    # Testing with dataset 55 and 68
    r = client.get('/dataset_info/using_multiple_discoverIds/?discoverIds=55&discoverIds=68')
    results = json.loads(r.data)['results']
    dataset_version = results[0]['version']
    if version.parse(dataset_version) >= version.parse("1.1.4"):
        discover_id_1 = results[0]['dataset_identifier']
        discover_id_2 = results[1]['dataset_identifier']
        assert discover_id_1 == "55" or discover_id_1 == "68"
        assert discover_id_2 == "55" or discover_id_2 == "68"

def test_scicrunch_search(client):
    r = client.get('/search/heart')
    assert r.status_code == 200
    assert json.loads(r.data)['numberOfHits'] > 4


def test_scicrunch_all_data(client):
    r = client.get('/filter-search/')
    assert json.loads(r.data)['numberOfHits'] > 40


def test_scicrunch_filter(client):
    r = client.get('/filter-search/', query_string={'term': 'organ', 'facet': 'heart'})
    assert json.loads(r.data)['numberOfHits'] > 4


def test_scicrunch_filter_scaffolds(client):
    r = client.get('/filter-search/?facet=scaffolds&term=datasets')
    assert json.loads(r.data)['numberOfHits'] > 10


def test_scicrunch_basic_search(client):
    r = client.get('/filter-search/Heart/?facet=All+Species&term=species')
    assert json.loads(r.data)['numberOfHits'] > 10


def test_scicrunch_boolean_logic(client):
    r = client.get('/filter-search/?facet=All+Species&term=species&facet=male&term=gender&facet=female&term=gender')
    assert json.loads(r.data)['numberOfHits'] > 20


def test_scicrunch_combined_facet_text(client):
    r = client.get('/filter-search/heart/?facet=All+Species&term=species&facet=male&term=gender&facet=female&term=gender')
    assert json.loads(r.data)['numberOfHits'] > 1


def test_getting_facets(client):
    r = client.get('/get-facets/organ')
    facet_results = json.loads(r.data)
    facets = [facet_result['key'] for facet_result in facet_results]
    assert 'heart' in facets


def test_create_identifier_query(client):
    r = client.get('/dataset_info/using_object_identifier?identifier=package:e6435710-dd9c-46b7-9dfd-932103469733')

    json_data = json.loads(r.data)
    assert 'result' in json_data

    results = json_data['result']
    assert len(results) == 1

    result = results[0]
    assert 'version' in result
    assert result['version'] == '1.1.5'

    assert 'title' in result
    assert result['title'] == 'Morphometric analysis of the abdominal vagus nerve in rats'


def test_response_version(client):
    # Testing with dataset 44
    identifier = "44"
    doi = "10.26275/duz8-mq3n"
    run_doi_test = check_doi_status(client, identifier, doi)
    if run_doi_test:
        r = client.get('/dataset_info/using_doi', query_string={'doi': doi})
        data = r.data.decode('utf-8')
        json_data = json.loads(data)
        assert len(json_data['result']) == 1
        assert 'version' in json_data['result'][0]
    else:
        pytest.skip('DOI used in test is out of date.')


def test_response_abi_plot(client):
    # Testing abi-plot with dataset 141
    identifier = "141"
    doi = "10.26275/9qws-u3px"
    run_doi_test = check_doi_status(client, identifier, doi)
    if run_doi_test:
        r = client.get('/dataset_info/using_doi', query_string={'doi': doi})
        data = r.data.decode('utf-8')
        json_data = json.loads(data)
        assert len(json_data['result']) == 1
        if json_data['result'][0]['version'] == '1.1.5':
            assert len(json_data['result'][0]['abi-plot']) == 21
            identifier = json_data['result'][0]["dataset_identifier"]
            version = json_data['result'][0]["dataset_version"]
            assert identifier == "141"
            assert version == "3"
            # Construct the file path prefix, it should be /exists/141/3/files
            path_prefix = '/'.join(('', 'exists', identifier, version, 'files'))
            for plot in json_data['result'][0]['abi-plot']:
                for path in  plot['datacite']['isDescribedBy']['path']:
                    if path:
                        path = '/'.join((path_prefix, path))
                        # Check if the file exists using the /exists/{path} route
                        r2 = client.get(path)
                        data2 = r2.data.decode('utf-8')
                        json_data2 = json.loads(data2)
                        print(path)
                        assert json_data2['exists'] == 'true'
        else:
            pytest.skip('Only test abi-plot against version 1.1.5.')
    else:
        pytest.skip('DOI used in test is out of date.')


def test_response_abi_scaffold(client):
    # Testing abi-scaffold with dataset 76
    identifier = "76"
    doi = "10.26275/jarb-s8jw"
    run_doi_test = check_doi_status(client, identifier, doi)
    if run_doi_test:
        r = client.get('/dataset_info/using_doi', query_string={'doi': doi})
        data = r.data.decode('utf-8')
        json_data = json.loads(data)
        if len(json_data['result']) == 1:
            if json_data['result'][0]['version'] == '1.1.5':
                identifier = json_data['result'][0]["dataset_identifier"]
                dataset_version = json_data['result'][0]["dataset_version"]
                assert identifier == "76"
                assert dataset_version == "4"
                # Construct the file path prefix, it should be /exists/76/4/files
                path_prefix = '/'.join(('', 'exists', identifier, dataset_version, 'files'))
                assert len(json_data['result'][0]['abi-scaffold-metadata-file']) == 1
                for plot in json_data['result'][0]['abi-scaffold-metadata-file']:
                    for path in plot['datacite']['isSourceOf']['path']:
                        if path:
                            path = '/'.join((path_prefix, path))
                            # Check if the file exists using the /exists/{path} route
                            r2 = client.get(path)
                            data2 = r2.data.decode('utf-8')
                            json_data2 = json.loads(data2)
                            print(path)
                            assert json_data2['exists'] == 'true'

                assert len(json_data['result'][0]['abi-scaffold-view-file']) == 4
                for plot in json_data['result'][0]['abi-scaffold-view-file']:
                    for path in plot['datacite']['isSourceOf']['path']:
                        if path:
                            path = '/'.join((path_prefix, path))
                            # Check if the file exists using the /exists/{path} route
                            r2 = client.get(path)
                            data2 = r2.data.decode('utf-8')
                            json_data2 = json.loads(data2)
                            print(path)
                            assert json_data2['exists'] == 'true'

                assert len(json_data['result'][0]['abi-scaffold-thumbnail']) == 4         
                for plot in json_data['result'][0]['abi-scaffold-thumbnail']:
                    for path in plot['datacite']['isDerivedFrom']['path']:
                        if path:
                            path = '/'.join((path_prefix, path))
                            # Check if the file exists using the /exists/{path} route
                            r2 = client.get(path)
                            data2 = r2.data.decode('utf-8')
                            json_data2 = json.loads(data2)
                            print(path)
                            assert json_data2['exists'] == 'true'
            else:
                pytest.skip('Only test abi-plot against version 1.1.5.')
        else:
            pytest.skip('DOI used in test is out of date.')


def test_response_sample_subject_size(client):
    # Only filter search returns the sample and subjectSuze
    r = client.get('/filter-search/?facet=liver&term=organ')
    data = r.data.decode('utf-8')
    json_data = json.loads(data)
    print(json_data)
    assert len(json_data['results']) == 1
    assert json_data['results'][0]['sampleSize'] == '9'
    assert json_data['results'][0]['subjectSize'] == '9'


source_structure = {
    'type': dict,
    'required': ['contributors', 'dataItem',
                 {'item':
                     {
                         'type': dict,
                         'required': ['types', 'contentTypes', 'statistics', 'keywords', 'published',
                                      'description',
                                      'name', 'identifier', 'docid', 'curie'],
                         'optional': [{'version': {'type': dict, 'required': ['keyword'], 'optional': []}},
                                       'techniques', 'readme', 'modalities', 'names']
                     }}, 'pennsieve', 'provenance', 'supportingAwards'],
    'optional': ['anatomy', 'attributes', 'dates', 'diseases', 'distributions',
                 {'objects':
                     {
                         'type': list,
                         'item': {
                             'type': dict,
                             'required': ['bytes', 'dataset', 'identifier', 'mimetype', 'name', 'updated'],
                             'optional': [ 'distributions' ]}
                     }
                 }, 'organisms', 'protocols', 'publication', 'xrefs']
}
raw_structure_base = {
    'type': dict,
    'required': [
        {'hits': {
            'type': dict,
            'required': [
                {'hits':
                     {'type': list,
                      'item': {
                          'type': dict,
                          'required': ['_index', '_type', '_id', '_score',
                                       {'_source': source_structure}
                                       ],
                          'optional': ['_ignored']}
                      }
                 }
            ],
            'optional': [],
        }
        }
    ],
    'optional': []
}


class StructureDefinitionError(Exception):
    pass


def _test_sub_structure(data, structure, required=True):
    for st in structure:
        if isinstance(st, str):
            if required and st not in data:
                print(f'failed: {st}')
                return False

            continue

        # req should have exactly one key
        if not len(st.keys()) == 1:
            raise StructureDefinitionError

        key = next(iter(st))
        if required and key not in data:
            print(f'key failed: {key}')
            return False

        # if key == '_source':
        #     a = list(data[key].keys())
        #     a.sort()
        #     print(a)
        if key in data and not _test_structure(data[key], st[key]):
            print(f'structure failed: {key} - {st[key]["type"]}, {type(data[key])} - {st[key]} - {len(data[key])}')
            return False

    return True


def _test_structure(data, structure):
    structure_type = structure['type']
    # print('=============================')
    # print(structure)
    if isinstance(data, structure_type):
        if structure_type is dict:
            if not _test_sub_structure(data, structure['required'], required=True):
                return False

            if not _test_sub_structure(data, structure['optional'], required=False):
                return False
        elif structure_type is list:
            for list_item in data:
                if not _test_structure(list_item, structure['item']):
                    return False
        else:
            print('type if not dict or list', type(data))

        return True

    return False


def test_raw_response_structure(client):
    # 10.26275/zdxd-84xz
    # 10.26275/duz8-mq3n
    query = create_query_string("computational")
    data = dataset_search(query)
    # print(data['hits']['hits'][0]['_source']['objects'])
    # print(data['hits']['hits'][0]['_source']['item'])
    assert _test_structure(data, raw_structure_base)
    assert 'hits' in data
    assert 'hits' in data['hits']
    assert isinstance(data['hits']['hits'], list)
    for hit in data['hits']['hits']:
        if 'version' in hit['_source']['item']:
            print(hit['_source']['item']['version']['keyword'])
        else:
            print('no version')

    for hit in data['hits']['hits']:
        print(hit['_source'].keys())
    objects = data['hits']['hits'][0]['_source']['objects']
    for o in objects:
        mimetype = o.get('mimetype', 'not-specified').get('name', 'no-name')
        # print('mimetype: ', mimetype)
        if mimetype == 'image/png':
            # print(o)
            print('.', end="")

    print()
    # for k in data['hits']['hits'][0]:
    #     print(k, data['hits']['hits'][0][k])


def test_getting_curies(client):
    # Test if we get a shorter list of uberons with species specified
    r = client.get('/get-organ-curies/')
    uberons_results = json.loads(r.data)
    total = len(uberons_results['uberon']['array'])
    assert total > 0
    r = client.get('/get-organ-curies/?species=human')
    uberons_results = json.loads(r.data)
    human = len(uberons_results['uberon']['array'])
    assert total > human
    # Test if the uberon - name match the one from the hardcoded list
    for item in uberons_results['uberon']['array']:
        if item['id'] in UBERONS_DICT:
            assert UBERONS_DICT[item['id']] == item['name'].lower()


def test_get_related_terms(client):
    # Test if we can get the uberon term of heart using the uberon term
    # of left ventricle
    r = client.get('/get-related-terms/UBERON:0002084')
    uberons_results = json.loads(r.data)
    print(uberons_results)
    total = len(uberons_results['uberon']['array'])
    assert total > 0
    findHeart = False
    for item in uberons_results['uberon']['array']:
        if item['id'] == 'UBERON:0000948' and item['name'] == 'heart':
            findHeart = True
            break
    assert findHeart == True


def test_scaffold_files(client):
    r = client.get('/filter-search/?size=30')
    results = json.loads(r.data)
    assert results['numberOfHits'] > 0
    for item in results['results']:
        if 'abi-scaffold-metadata-file' in item and 's3uri' in item:
            uri = item['s3uri']
            path = item['abi-scaffold-metadata-file'][0]['dataset']['path']
            key = f"{uri}files/{path}".replace('s3://pennsieve-prod-discover-publish-use1/', '')
            r = client.get(f"/s3-resource/{key}")
            assert r.status_code == 200

def test_finding_contextual_information(client):
    r = client.get('/dataset_info/using_multiple_discoverIds/?discoverIds=76')
    results = json.loads(r.data)
    assert results['numberOfHits'] > 0  # Test we could find the generic colon scaffold dataset
    for item in results['results']:
        assert len(item['abi-contextual-information']) > 0  # Check it has contextual information

def test_undefined_version_dataset_search(client):
    # Testing with dataset 17 which is not versioned
    identifier = "17"
    doi = "10.26275/mlua-o9oj"
    r = client.get('/dataset_info/using_doi', query_string={'doi': doi})
    data = r.data.decode('utf-8')
    json_data = json.loads(data)
    assert len(json_data['result']) == 1
    assert 'dataset_identifier' in json_data['result'][0]
    assert json_data['result'][0]['dataset_identifier'] == '17'
