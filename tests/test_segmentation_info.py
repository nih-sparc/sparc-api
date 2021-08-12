import json
import pytest
from app import app
from app.main import dataset_search
from app.scicrunch_requests import create_query_string


@pytest.fixture
def client():
    # Spin up test flask app
    app.config['TESTING'] = True
    return app.test_client()


def test_segmentation_info(client):
    file_path = '43/5/files/derivative/sub-6384/sam-28_sub-6384_islet3/sub-6384_20x_MsGcg_RbCol4_SMACy3_islet3 (1).xml'
    r = client.get('/segmentation_info/', query_string={'path': file_path})
    assert r.status_code == 200

    json_data = r.json
    assert 'atlas' in json_data
    assert 'subject' in json_data

    assert json_data['atlas']['organ'] == 'Pancreas'
