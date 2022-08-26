import json
import pytest
from app import app


@pytest.fixture
def client():
    app.config['TESTING'] = True
    return app.test_client()


def test_osparc_no_post(client):
    r = client.get('/simulation')
    assert r.status_code == 405


def test_osparc_empty_post(client):
    r = client.post("/simulation", json={})
    assert r.status_code == 400


def test_osparc_no_opencor_settings(client):
    data = {
        "solver_name": "simcore/services/comp/opencor",
        "solver_version": "1.0.3"
    }
    r = client.post("/simulation", json=data)
    assert r.status_code == 400


def test_osparc_no_osparc_settings(client):
    data = {
        "solver_name": "simcore/services/comp/rabbit-ss-0d-cardiac-model",
        "solver_version": "1.0.1"
    }
    r = client.post("/simulation", json=data)
    assert r.status_code == 400


def test_osparc_valid_data(client):
    data = {
        "solver_name": "simcore/services/comp/opencor",
        "solver_version": "1.0.3",
        "opencor": {
            "model_url": "https://models.physiomeproject.org/e/611/HumanSAN_Fabbri_Fantini_Wilders_Severi_2017.cellml",
            "json_config": {
                "simulation": {
                    "Ending point": 0.003,
                    "Point interval": 0.001,
                },
                "output": ["Membrane/V"]
            }
        }
    }
    res = {
        "status": "ok",
        "results": {
            "environment/time": [0.0, 0.001, 0.002, 0.003],
            "Membrane/V": [-47.787168, -47.74547155339473, -47.72515226841376, -47.71370033208329]
        }
    }
    r = client.post("/simulation", json=data)
    assert r.status_code == 200
    assert json.dumps(json.loads(r.data), sort_keys=True) == json.dumps(res, sort_keys=True)


def test_osparc_failing_simulation(client):
    data = {
        "solver_name": "simcore/services/comp/opencor",
        "solver_version": "1.0.3",
        "opencor": {
            "model_url": "https://models.physiomeproject.org/e/611/HumanSAN_Fabbri_Fantini_Wilders_Severi_2017.cellml",
            "json_config": {
                "simulation": {
                    "Ending point": 3.0,
                    "Point interval": 1.0,
                },
                "output": ["Membrane/V"]
            }
        }
    }
    res = {
        "status": "nok",
        "description": "the simulation failed"
    }
    r = client.post("/simulation", json=data)
    assert r.status_code == 200
    assert json.dumps(json.loads(r.data), sort_keys=True) == json.dumps(res, sort_keys=True)
