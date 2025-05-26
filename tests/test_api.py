import pytest
from app import app
from app.config import Config
import requests
import random
import string
import hmac
import hashlib
import base64
import time
import json

from requests.auth import HTTPBasicAuth


@pytest.fixture
def client():
    # Spin up test flask app
    app.config['TESTING'] = True
    return app.test_client()


def test_direct_download_url_small_file(client):
    small_s3_file = '217/files/derivative/brainstem_pig_metadata.json'
    r = client.get(f"/s3-resource/{small_s3_file}?s3BucketName=prd-sparc-discover50-use1")

    assert r.status_code == 200
    assert b"medulla" in r.data

def test_direct_download_url_new_bucket_file(client):
    new_s3_file = '307%2Ffiles%2Fderivative%2Fhuman_body_metadata.json'
    r = client.get(f"/s3-resource/{new_s3_file}?s3BucketName=prd-sparc-discover50-use1")

    assert r.status_code == 200
    assert b"colon" in r.data


def test_direct_download_url_thumbnail(client):
    small_s3_file = '95/files/derivative%2FcolonHuman_Layout1_thumbnail.jpeg'
    r = client.get(f"/s3-resource/{small_s3_file}?s3BucketName=prd-sparc-discover50-use1")

    assert r.status_code == 200
    assert b"\xFF\xD8\xFF" in r.data

def test_direct_download_incorrect_path(client):
    incorrect_path = '95/files/?encodeBase64=true'
    r = client.get(f"/s3-resource/{incorrect_path}?s3BucketName=prd-sparc-discover50-use1")

    assert r.status_code != 200

def test_direct_download_empty_path(client):
    r = client.get(f"/s3-resource/")

    assert r.status_code == 404


def test_direct_download_url_large_file(client):
    large_s3_file = '61%2Ffiles%2Fprimary%2Fsub-44%2Fsam-1%2Fmicroscopy%2Fsub-44sam-1C44-1Slide2p2MT_10x.nd2'
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


def test_annotation_get_share_id_and_state(client):
    # mock json for testing
    test_data = { "state" : [
        {
            "resource":"placeholder1",
            "item":{"id":"123"},
            "body":{"evidence":["https://doi.org/caxdd"],
            "comment":"asdac",
            "type":"connectivity",
            "source":{"label":"body proper","id":1002,"models":"UBERON:0013702"},
            "target":{"label":"body proper","id":1002,"models":"UBERON:0013702"},
            "intermediates":[]},
            "feature":{
                "id":"safsfa","type":"Feature",
                "properties":{"drawn":True,"label":"Drawn annotation"},
                "geometry":{
                    "coordinates":[
                        [-12.148524690634474,-12.730414964960303],
                        [-22.302217020303743,-6.678936298958405]
                    ],
                    "type":"LineString"},
            "connection":{" 1002":{"label":"body proper","id":1002,"models":"UBERON:0013702"}}}
        },
        {
            "resource":"placeholder2","item":{"id":"__annotation/LineString"},
            "body":{"evidence":[],"comment":"Create"},
            "feature":{"id":"__annotation/LineString",
                "properties":{"drawn":True,"label":"Drawn annotation"},
                "geometry":{
                    "coordinates":[
                        [10.914859771728516,3.357909917831421,2.910676956176758],
                        [9.065815925598145,13.387456893920898,-24.09609031677246]
                    ],
                    "type":"MultiLineString"
                }
            },
            "group":"LineString","region":"__annotation"
        }
    ]}

    r = client.post(f"/annotation/getshareid", json = {})
    assert r.status_code == 400

    r = client.post(f"/annotation/getshareid", json = test_data)
    assert r.status_code == 200
    assert "uuid" in r.get_json()

    r = client.post(f"/annotation/getstate", json = r.get_json())
    assert r.status_code == 200
    returned_data = r.get_json()
    assert "state" in returned_data
    assert len(returned_data["state"]) == 2
    assert returned_data["state"][0]['resource'] == "placeholder1"
    assert returned_data["state"][1]['resource'] == "placeholder2"

    r = client.post(f"/map/getstate", json = {"uuid": "1234567"})
    assert r.status_code == 400

    r = client.post(f"/map/getstate", json = {})
    assert r.status_code == 400


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

def test_get_hubspot_contact(client):
    r = client.get(f"/hubspot_contact_properties/hubspot_webhook_test@test.com")
    assert r.status_code == 200

def test_subscribe_to_newsletter(client):
    http_method = "POST"
    endpoint = "/subscribe_to_newsletter"
    base_url = "http://localhost"  # Default for Flask test client
    full_url = f"{base_url}{endpoint}"
    mock_body = {"email_address":"hubspot_webhook_test@test.com","first_name":"Test Hubspot Webhook","last_name":"Do Not Delete"}
    response = client.post(
        endpoint,
        json=mock_body,
        headers={
            "Content-Type": "application/json"
        }
    )
    assert response.status_code == 200

def test_hubspot_webhook(client):
    http_method = "POST"
    endpoint = "/hubspot_webhook"
    base_url = "http://localhost"  # Default for Flask test client
    full_url = f"{base_url}{endpoint}"
    # mock a property changed event firing for test Hubspot contact
    mock_body = [{"subscriptionType":"contact.propertyChange","objectId":"83944215465"}]
    # The timestamp must be a Unix epoch time within 5 minutes (300 seconds) of the current time when the webhook request is received.
    valid_timestamp = int(time.time())
    # Concatenate the string as HubSpot does
    raw_json = json.dumps(mock_body, separators=(",", ":"))
    data_to_sign = f'{http_method}{full_url}{raw_json}{valid_timestamp}'

    # Generate the HMAC SHA256 signature
    signature = hmac.new(
        key=Config.HUBSPOT_CLIENT_SECRET.encode('utf-8'),
        msg=data_to_sign.encode('utf-8'),
        digestmod=hashlib.sha256
    ).digest()

    # Encode the signature in Base64
    mock_signature = base64.b64encode(signature).decode()
    # Send a mock POST request
    response = client.post(
        endpoint,
        data=raw_json,
        headers={
            "Content-Type": "application/json",
            "X-HubSpot-Signature-Version": "v3",
            "X-Hubspot-Signature-v3": mock_signature,
            "X-HubSpot-Request-Timestamp": str(valid_timestamp),
        }
    )

    assert response.status_code == 200

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


def test_non_existing_simulation_ui_file(client):
    r = client.get('/simulation_ui_file/137')
    assert r.status_code == 404


def test_simulation_ui_file_old_s3_bucket(client):
    r = client.get('/simulation_ui_file/135')
    assert r.status_code == 200
    assert r.get_json()['simulation']['solvers'][0]['name'] == 'simcore/services/comp/opencor'


def test_simulation_ui_file_new_s3_bucket(client):
    r = client.get('/simulation_ui_file/308')
    assert r.status_code == 200
    assert r.get_json()['simulation']['solvers'][0]['name'] == 'simcore/services/comp/kember-cardiac-model'


def test_get_featured_datasets(client):
    r = client.get('/get_featured_datasets_identifiers')
    assert r.status_code == 200
    json = r.get_json()
    assert 'identifiers' in json
    assert type(json['identifiers']) == list

def test_get_protocol_views(client):
    r = client.get('/total_protocol_views')
    assert r.status_code in (200, 202)
    json = r.get_json()
    assert 'total_views' in json

def test_get_total_dataset_citations(client):
    r = client.get('/total_dataset_citations')
    assert r.status_code == 200
    json = r.get_json()
    assert 'total_citations' in json

def test_get_reva_subject_ids(client):
    r = client.get('/reva/subject-ids')
    assert r.status_code == 200
    json = r.get_json()
    assert 'ids' in json
    assert type(json['ids']) == list

def test_get_reva_tracing_files(client):
    r = client.get('/reva/tracing-files/sub-SR005')
    assert r.status_code == 200
    json = r.get_json()
    assert 'files' in json
    assert type(json['files']) == list

def test_get_reva_micro_ct_files(client):
    r = client.get('/reva/micro-ct-files/sub-SR005')
    assert r.status_code == 200
    json = r.get_json()
    assert 'files' in json
    assert type(json['files']) == list

def test_get_reva_landmarks_files(client):
    r = client.get('/reva/anatomical-landmarks-files/sub-SR005')
    assert r.status_code == 200
    json = r.get_json()
    assert 'folders' in json
    assert type(json['folders']) == list

def test_create_issue(client):
    create_response = client.post("/create_issue", data={
        "title": "test-sparc-api-issue-creation",
        "body": "This is a test generated from the sparc-api test suite. This ticket should be automatically closed, but if it is not then please do so"
    })

    create_response_json = create_response.get_json()

    assert create_response.status_code == 201
    assert create_response_json['status'] == 'success'

    issue_api_url = create_response_json['issue_api_url']
    assert issue_api_url is not None

    headers = {
        "Authorization": f"token {Config.SPARC_TECH_LEADS_GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json"
    }

    close_response = requests.patch(
        issue_api_url,
        headers=headers,
        json={"state": "closed", "labels": ['test']}
    )

    assert close_response.status_code == 200
