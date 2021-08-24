import pytest
from app import app


@pytest.fixture
def client():
    # Spin up test flask app
    app.config['TESTING'] = True
    return app.test_client()


def test_segmentation_thumbnail(client):
    r = client.get('/thumbnail/segmentation?path=37/2/files/derivative/sub-54-5/TJU_3Scan_ratheart54-5_updated_06_11_19_Fiducials.xml')
    assert r.data.decode('utf-8').startswith('data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAEAAAABACAIAAAAlC+aJAAAFIElEQVR4nO1ZTYgcRRT+aiaaZTCXDegl')


def test_neurolucida_thumbnail(client):
    query_string = {'datasetId': 37, 'version': 2, 'path': 'files/derivative/sub-54-5/TJU_3Scan_ratheart54-5_updated_06_11_19_Fiducials.xml'}
    r = client.get('/thumbnail/neurolucida', query_string=query_string)

    assert r.data.decode('utf-8').startswith('iVBORw0KGgoAAAANSUhEUgAAAtAAAAIcCAYAAADffZlTAAAAAXNSR0IArs4c6QAAIABJREFUeJzsvS97')
