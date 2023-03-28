import pytest
from app import app


@pytest.fixture
def client():
    # Spin up test flask app
    app.config['TESTING'] = True
    return app.test_client()


def test_segmentation_info(client):
    file_path = '43/5/files/derivative/sub-6384/sam-28_sub-6384_islet3/sub-6384_20x_MsGcg_RbCol4_SMACy3_islet3 (1).xml'
    r = client.get('/segmentation_info/', query_string={'dataset_path': file_path})
    assert r.status_code == 200

    json_data = r.json
    assert 'atlas' in json_data
    assert 'subject' in json_data

    assert json_data['atlas']['organ'] == 'Pancreas'


def test_segmentation_info_namespaced(client):
    file_path = '230/1/files/primary/sub-dorsal-4/sam-CGRP-Mouse-Dorsal-4/3D_scaffold_-_CGRP-Mice-Dorsal-4.xml'
    r = client.get('/segmentation_info/', query_string={'dataset_path': file_path})
    assert r.status_code == 200

    json_data = r.json
    assert 'atlas' in json_data
    assert 'subject' in json_data

    assert json_data['atlas']['organ'] == 'Stomach'
