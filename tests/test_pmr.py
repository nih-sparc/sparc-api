import pytest
from app import app


@pytest.fixture
def client():
    app.config['TESTING'] = True
    return app.test_client()


def test_pmr_file(client):
    r = client.get('/pmr_file')
    assert r.status_code == 405


def test_pmr_file_empty_post(client):
    r = client.post("/pmr_file", json={})
    assert r.status_code == 400


def test_pmr_file_valid_path(client):
    r = client.post("/pmr_file", json={"path": "workspace/486/rawfile/55879cbc485e2d4c41f3dc6d60424b849f94c4ee/HumanSAN_Fabbri_Fantini_Wilders_Severi_2017.cellml"})
    assert r.status_code == 200


def test_pmr_file_invalid_path(client):
    r = client.post("/pmr_file", json={"path": "invalid_path"})
    assert r.status_code == 400


def test_pmr_latest_exposure_no_post(client):
    r = client.get('/pmr_latest_exposure')
    assert r.status_code == 405


def test_pmr_latest_exposure_empty_post(client):
    r = client.post("/pmr_latest_exposure", json={})
    assert r.status_code == 400


def test_pmr_latest_exposure_workspace_with_latest_exposure(client):
    r = client.post("/pmr_latest_exposure", json={"workspace_url": "https://models.physiomeproject.org/workspace/486"})
    assert r.status_code == 200
    data = r.get_json()
    assert data["url"] == "https://models.physiomeproject.org/e/611"


def test_pmr_latest_exposure_workspace_without_latest_exposure(client):
    r = client.post("/pmr_latest_exposure", json={"workspace_url": "https://models.physiomeproject.org/workspace/698"})
    assert r.status_code == 200
    data = r.get_json()
    assert data["url"] == ""


def test_pmr_latest_exposure_workspace_with_invalid_workspace_url(client):
    r = client.post("/pmr_latest_exposure", json={"workspace_url": "https://some.url.com/"})
    assert r.status_code == 400
