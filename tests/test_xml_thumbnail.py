import pytest
from app import app


@pytest.fixture
def client():
    # Spin up test flask app
    app.config['TESTING'] = True
    return app.test_client()


def test_xml_thumbnail(client):
    r = client.get('/xml-thumbnail/37/2/files/derivative/sub-54-5/TJU_3Scan_ratheart54-5_updated_06_11_19_Fiducials.xml')
    assert r.data.startswith(b'data:image/png;base64,iVBORw0KGgoAAAANSUhEUg')
