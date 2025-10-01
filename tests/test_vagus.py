import pytest

from app import app
from app.scicrunch_process_results import reform_flatmap_query_result


from app.scicrunch_requests import create_dataset_flatmap_query


@pytest.fixture
def client():
    # Spin up test flask app
    app.config['TESTING'] = True
    return app.test_client()


def test_find_flatmap_uuid(client):
    target_subject = 'sub-f006'
    target_dataset = '2a3d01c0-39d3-464a-8746-54c9d67ebe0f'
    r = client.get('/flatmap/find', query_string={'subject': target_subject, 'dataset': target_dataset})
    assert r.status_code == 200
    expected_result = [{'dataset': 'N:dataset:031598b5-88eb-44eb-ba70-67ad1c2fe36a', 'left': '0ea45841-ce99-5b86-8d16-2d12689566f6', 'right': '238599e8-fd25-533e-9f88-68fad90c1bf2', 'subject': 'sub-f006'}]
    assert expected_result == r.json


def test_find_flatmap_uuid_bad_subject(client):
    target_subject = 'sub-f999'
    target_dataset = '2a3d01c0-39d3-464a-8746-54c9d67ebe0f'
    r = client.get('/flatmap/find', query_string={'subject': target_subject, 'dataset': target_dataset})
    assert r.status_code == 404
    expected_result = {'error': f"404 Not Found: No results for subject '{target_subject}' in dataset '{target_dataset}'."}
    assert expected_result == r.json


def test_find_flatmap_uuid_bad_dataset(client):
    target_subject = 'sub-f006'
    target_dataset = '2b3d01c0-39d3-464a-8746-54c9d67ebe0f'
    r = client.get('/flatmap/find', query_string={'subject': target_subject, 'dataset': target_dataset})
    assert r.status_code == 404
    expected_result = {'error': f"404 Not Found: No results for subject '{target_subject}' in dataset '{target_dataset}'."}
    assert expected_result == r.json


def test_find_flatmap_uuid_missing_params(client):
    target_dataset = '2b3d01c0-39d3-464a-8746-54c9d67ebe0f'
    r = client.get('/flatmap/find', query_string={'dataset': target_dataset})
    assert r.status_code == 400
    assert 'Query arguments are not valid' in r.data.decode('utf-8')


def test_flatmap_query():
    query = create_dataset_flatmap_query("12345-6789-123-456")
    assert query['query']['match']['item.identifier']['query'] == '12345-6789-123-456'


def test_flatmap_query_named_parameters():
    query = create_dataset_flatmap_query("12345-6789-123-456", size=12, from_=6)
    assert query['size'] == 12
    assert query['from'] == 6
    assert query['query']['match']['item.identifier']['query'] == '12345-6789-123-456'


def test_reform_flatmap_query_result_bad_input():
    output = reform_flatmap_query_result(None, 'sub-f005', '12345-6789-123-45')
    assert output == {}


def test_reform_flatmap_query_result_empty_input():
    output = reform_flatmap_query_result({}, 'sub-f005', '12345-6789-123-45')
    assert output == {'dataset': '12345-6789-123-45', 'subject': 'sub-f005'}


def test_reform_flatmap_query_result_no_hits_input():
    output = reform_flatmap_query_result({'hits': {}}, 'sub-f005', '12345-6789-123-45')
    assert output == {'dataset': '12345-6789-123-45', 'subject': 'sub-f005'}


def test_reform_flatmap_query_result_valid_input():
    sci_crunch_data = {
        'hits': {
            'hits': [
                {
                    '_source': {
                        'objects': [
                            {
                                'associated_flatmap': {'identifier': '54321-6789-123-45'},
                                'dataset': {'path': '/home/sub-f005/L/here'}
                            },
                        ]
                    }
                }
            ]
        }
    }

    output = reform_flatmap_query_result(sci_crunch_data, 'sub-f005', '12345-6789-123-45')
    assert output == {'dataset': '12345-6789-123-45', 'subject': 'sub-f005', 'left': '54321-6789-123-45'}
