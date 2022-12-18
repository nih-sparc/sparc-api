import json
import pytest
from app import app


@pytest.fixture
def client():
    app.config['TESTING'] = True
    return app.test_client()


def test_osparc_start_simulation_no_post(client):
    r = client.get('/start_simulation')
    assert r.status_code == 405


def test_osparc_check_simulation_no_post(client):
    r = client.get('/check_simulation')
    assert r.status_code == 405


def test_osparc_start_simulation_empty_post(client):
    r = client.post("/start_simulation", json={})
    assert r.status_code == 400


def test_osparc_check_simulation_empty_post(client):
    r = client.post("/check_simulation", json={})
    assert r.status_code == 400


def test_osparc_start_simulation_no_data(client):
    data = {
    }
    r = client.post("/start_simulation", json=data)
    assert r.status_code == 400


def test_osparc_start_simulation_no_opencor_data(client):
    data = {
        "solver": {
            "name": "simcore/services/comp/opencor",
            "version": "1.0.3"
        }
    }
    r = client.post("/start_simulation", json=data)
    assert r.status_code == 400


def test_osparc_start_simulation_no_osparc_data(client):
    data = {
        "solver": {
            "name": "simcore/services/comp/rabbit-ss-0d-cardiac-model",
            "version": "1.0.1"
        }
    }
    r = client.post("/start_simulation", json=data)
    assert r.status_code == 400


def test_osparc_check_simulation_no_job_id_data(client):
    data = {
        "solver": {
            "name": "simcore/services/comp/opencor",
            "version": "1.0.3"
        }
    }
    r = client.post("/check_simulation", json=data)
    assert r.status_code == 400


def test_osparc_check_simulation_no_solver_data(client):
    data = {
        "job_id": "5026ff74-dc6d-4547-9166-6ae26d04b92e"
    }
    r = client.post("/check_simulation", json=data)
    assert r.status_code == 400


def test_osparc_check_simulation_no_solver_name_data(client):
    data = {
        "job_id": "5026ff74-dc6d-4547-9166-6ae26d04b92e",
        "solver": {
            "version": "1.0.3"
        }
    }
    r = client.post("/check_simulation", json=data)
    assert r.status_code == 400


def test_osparc_check_simulation_no_solver_version_data(client):
    data = {
        "job_id": "5026ff74-dc6d-4547-9166-6ae26d04b92e",
        "solver": {
            "name": "simcore/services/comp/opencor"
        }
    }
    r = client.post("/check_simulation", json=data)
    assert r.status_code == 400


def test_osparc_check_simulation_no_data(client):
    data = {
    }
    r = client.post("/check_simulation", json=data)
    assert r.status_code == 400


def test_osparc_successful_simulation(client):
    data = {
        "opencor": {
            "model_url": "https://models.physiomeproject.org/e/611/HumanSAN_Fabbri_Fantini_Wilders_Severi_2017.cellml",
            "json_config": {
                "simulation": {
                    "Ending point": 0.003,
                    "Point interval": 0.001,
                },
                "output": ["Membrane/V"]
            }
        },
        "solver": {
            "name": "simcore/services/comp/opencor",
            "version": "1.0.3"
        }
    }
    res = {
        "status": "ok",
        "results": {
            "environment/time": [0.0, 0.001, 0.002, 0.003],
            "Membrane/V": [-47.787168, -47.74547155339473, -47.72515226841376, -47.71370033208329]
        }
    }
    r = client.post("/start_simulation", json=data)
    assert r.status_code == 200
    check_simulation_data = json.loads(r.data)["data"]
    while True:
        r = client.post("/check_simulation", json=check_simulation_data)
        assert r.status_code == 200
        json_data = json.loads(r.data)
        assert json_data["status"] == "ok"
        if "results" in json_data:
            assert json.dumps(json_data, sort_keys=True) == json.dumps(res, sort_keys=True)
            break


def test_osparc_failing_simulation(client):
    data = {
        "opencor": {
            "model_url": "https://models.physiomeproject.org/e/611/HumanSAN_Fabbri_Fantini_Wilders_Severi_2017.cellml",
            "json_config": {
                "simulation": {
                    "Ending point": 3.0,
                    "Point interval": 1.0,
                },
                "output": ["Membrane/V"]
            }
        },
        "solver": {
            "name": "simcore/services/comp/opencor",
            "version": "1.0.3"
        }
    }
    res = {
        "status": "nok",
        "description": "the simulation failed"
    }
    r = client.post("/start_simulation", json=data)
    assert r.status_code == 200
    check_simulation_data = json.loads(r.data)["data"]
    while True:
        r = client.post("/check_simulation", json=check_simulation_data)
        assert r.status_code == 200
        json_data = json.loads(r.data)
        if json_data["status"] == "nok":
            assert json.dumps(json_data, sort_keys=True) == json.dumps(res, sort_keys=True)
            break
