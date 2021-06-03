import json
import pytest
from app import app

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

def test_scicrunch_filter_scaffolds(client):
    r = client.get('/filter-search/?facet=scaffolds&term=datasets')
    assert json.loads(r.data)['numberOfHits'] > 10

def test_scicrunch_filter_simulations(client):
    r = client.get('/filter-search/?facet=simulations&term=datasets')
    assert json.loads(r.data)['numberOfHits'] > 0

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
