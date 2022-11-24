import pytest
from app import app
from app.config import Config
import requests
import random
import string

from requests.auth import HTTPBasicAuth


@pytest.fixture
def client():
    # Spin up test flask app
    app.config['TESTING'] = True
    return app.test_client()


def test_direct_download_url_small_file(client):
    small_s3_file = '76%2F2%2Ffiles%2Fderivative%2FScaffold%2FmouseColon_metadata.json'
    r = client.get(f"/s3-resource/{small_s3_file}")

    assert r.status_code == 200
    assert b"proximal colon" in r.data


def test_direct_download_url_thumbnail(client):
    small_s3_file = '95/1/files/derivative%2FScaffold%2Fthumbnail.png'
    r = client.get(f"/s3-resource/{small_s3_file}")

    assert r.status_code == 200
    assert b"PNG" in r.data


def test_direct_download_url_large_file(client):
    large_s3_file = '61%2F2%2Ffiles%2Fprimary%2Fsub-44%2Fsam-1%2Fmicroscopy%2Fsub-44sam-1C44-1Slide2p2MT_10x.nd2'
    r = client.get(f"/s3-resource/{large_s3_file}")

    assert r.status_code == 413
    assert 'File too big to download' in r.get_data().decode()


def test_get_owner_email(client):
    # SPARC Portal user info
    portal_user_id = 729
    portal_user_email = 'nih-data-portal@blackfynn.com'

    r = client.get(f"/get_owner_email/{portal_user_id}")
    assert r.status_code == 200
    assert r.get_json()['email'] == portal_user_email

    r = client.get(f"/get_owner_email/{999999}")
    assert r.status_code == 404


def test_get_datasets_by_project(client):
    # SPARC Portal project info
    portal_project_id = 'OT2OD025340'

    r = client.get(f"/project/{999999}")
    assert r.status_code == 404

    r = client.get(f"/project/{portal_project_id}")
    assert r.status_code == 200


def test_map_get_share_id_and_state(client):
    # mock json for testing
    test_data = { "state" : { "type" : "scaffold", "value": 1234 } }

    r = client.post(f"/map/getshareid", json = {})
    assert r.status_code == 400

    r = client.post(f"/map/getshareid", json = test_data)
    assert r.status_code == 200
    assert "uuid" in r.get_json()

    r = client.post(f"/map/getstate", json = r.get_json())
    assert r.status_code == 200
    returned_data = r.get_json()
    assert "state" in returned_data
    assert returned_data["state"]["type"] == "scaffold"
    assert returned_data["state"]["value"] == 1234

    r = client.post(f"/map/getstate", json = {"uuid": "1234567"})
    assert r.status_code == 400

    r = client.post(f"/map/getstate", json = {})
    assert r.status_code == 400


def test_scaffold_get_share_id_and_state(client):
    # mock json for testing
    testData = { "state" : { "far" : 12.32, "near": 0.23 } }

    r = client.post(f"/scaffold/getshareid", json = {})
    assert r.status_code == 400

    r = client.post(f"/scaffold/getshareid", json = testData)
    assert r.status_code == 200
    assert "uuid" in r.get_json()

    r = client.post(f"/scaffold/getstate", json = r.get_json())
    assert r.status_code == 200
    returned_data = r.get_json()
    assert "state" in returned_data
    assert returned_data["state"]["far"] == 12.32
    assert returned_data["state"]["near"] == 0.23

    r = client.post(f"/scaffold/getstate", json = {"uuid": "1234567"})
    assert r.status_code == 400

    r = client.post(f"/scaffold/getstate", json = {})
    assert r.status_code == 400


def test_create_wrike_task(client):
    
    r = client.post(f"/tasks", data={"title": "test-integration-task-sparc-api"})
    assert r.status_code == 400
    r2 = client.post(f"/tasks", data={"description": "test-integration-task-sparc-api<br />Here is a small text but not lorem ipsum"})
    assert r2.status_code == 400
    r3 = client.post(f"/tasks", data={"title": "test-integration-task-sparc-api", "description": "test-integration-task-sparc-api<br />Here is a small text but not lorem ipsum"})
    assert r3.status_code == 200

    # this part is only for cleaning the wrike board
    returned_data = r3.get_json()
    task_id = returned_data["task_id"]
    url = 'https://www.wrike.com/api/v4/tasks/{}'.format(task_id)
    hed = {'Authorization': 'Bearer ' + Config.WRIKE_TOKEN}
    resp = requests.delete(
        url=url,
        headers=hed
    )
    assert resp.status_code == 200


def test_subscribe_to_mailchimp(client):
    r = client.post(f"/mailchimp_subscribe", json={})
    assert r.status_code == 400

    letters = string.ascii_lowercase
    email = ''.join(random.choice(letters) for i in range(8))
    domain = ''.join(random.choice(letters) for i in range(6))

    email_address = '{}@{}.com'.format(email, domain)

    r2 = client.post(f"/mailchimp_subscribe", json={"email_address": email_address, "first_name": "Test", "last_name": "User"})
    assert r2.status_code == 200

    # this part is only for cleaning the mailchimp list and not pollute the mailing list
    returned_data = r2.get_json()
    member_hash = returned_data["id"]
    url = 'https://us2.api.mailchimp.com/3.0/lists/c81a347bd8/members/{}/actions/delete-permanent'.format(member_hash)
    auth = HTTPBasicAuth('AnyUser', Config.MAILCHIMP_API_KEY)
    resp = requests.post(
        url=url,
        auth=auth
    )
    assert resp.status_code == 204


def test_osparc_viewers(client):
    r = client.get('/get_osparc_data')
    assert r.status_code == 200
    osparc_data = r.get_json()
    assert 'file_viewers' in osparc_data


def test_sim_dataset(client):
    r = client.get('/sim/dataset/0')
    assert r.status_code == 404


def test_onto_term_lookup(client):
    r = client.get('/onto_term_lookup', query_string={'term': 'http://purl.obolibrary.org/obo/NCBITaxon_9606'})
    assert r.status_code == 200
    json_data = r.get_json()
    assert json_data['label'] == 'Human'


def test_non_existing_simualtion_ui_file(client):
    r = client.get('/simulation_ui_file/137')
    assert r.status_code == 404


def test_simualtion_ui_file(client):
    r = client.get('/simulation_ui_file/135')
    assert r.status_code == 200
    assert r.get_json()['input'][1]['enabled'] == '(sm == 1) || (sm == 2)'
