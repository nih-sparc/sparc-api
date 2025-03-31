import json

import pytest
from app import app


@pytest.fixture
def client():
    # Spin up test flask app
    app.config['TESTING'] = True
    return app.test_client()


def test_abi_plot(client):
    # Testing abi-plot with dataset 26
    dataset_id = "26"
    r = client.get('/dataset_info/using_pennsieve_identifier', query_string={'identifier': dataset_id})
    data = r.data.decode('utf-8')
    json_data = json.loads(data)

    assert len(json_data['result']) == 1
    assert len(json_data['result'][0]['abi-plot']) == 5
