import json
import pytest
from app import app
from app.main import create_doi_query, dataset_search


@pytest.fixture
def client():
    # Spin up test flask app
    app.config['TESTING'] = True
    return app.test_client()


def test_scicrunch_keys(client):
    r = client.get('/search/')
    assert r.status_code == 200
    assert 'numberOfHits' in json.loads(r.data).keys()


def test_scicrunch_search(client):
    r = client.get('/search/heart')
    assert r.status_code == 200
    assert json.loads(r.data)['numberOfHits'] > 4


def test_scicrunch_all_data(client):
    r = client.get('/filter-search/')
    assert json.loads(r.data)['numberOfHits'] > 40


def test_scicrunch_filter(client):
    r = client.get('/filter-search/', query_string={'term': 'genotype', 'facet': 'heart'})
    assert json.loads(r.data)['numberOfHits'] > 4


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
    r = client.get('/get-facets/genotype')
    facet_results = json.loads(r.data)
    facets = [facet_result['key'] for facet_result in facet_results]
    assert 'heart' in facets


def test_response_version(client):
    doi = "10.26275/duz8-mq3n"
    r = client.get('/dataset_info_from_doi/', query_string={'doi': doi})
    print(r.data)
    data = r.data
    assert 'version' in data


def test_raw_response_structure(client):
    # 10.26275/duz8-mq3n
    # 10.26275/zdxd-84xz
    # 10.26275/duz8-mq3n
    query = create_doi_query("10.26275/duz8-mq3n")
    data = dataset_search(query)
    print(data.keys())
    print(data['took'])
    print(data['hits']['total'])
    print(data['hits']['hits'][0].keys())
    print(data['hits']['hits'][0]['_source'].keys())
    print("===============")
    # print(data['hits']['hits'][0]['_source']['objects'])
    # print(data['hits']['hits'][0]['_source']['item'])
    if 'version' in data['hits']['hits'][0]['_source']['item']:
        print(data['hits']['hits'][0]['_source']['item']['version'])
    objs = data['hits']['hits'][0]['_source']['objects']
    for o in objs:
        mimetype = o.get('mimetype', 'not-specified')
        # print('mimetype: ', mimetype)
        if mimetype == 'image/png':
            print(o)

    # for k in data['hits']['hits'][0]:
    #     print(k, data['hits']['hits'][0][k])
