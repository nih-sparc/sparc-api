import pytest
from app import app


@pytest.fixture
def client():
    # Spin up test flask app
    app.config['TESTING'] = True
    return app.test_client()


def test_get_owner_email(client):
    # SPARC Portal user info
    portal_user_id = 729
    portal_user_email = 'nih-data-portal@blackfynn.com'

    r = client.get(f"/get_owner_email/{portal_user_id}")
    assert r.status_code == 200
    assert r.get_json()['email'] == portal_user_email

    r = client.get(f"/get_owner_email/{999999}")
    assert r.status_code == 404


def test_direct_download_url_small_file(client):
    small_s3_file = '76%2F2%2Ffiles%2Fderivative%2FScaffold%2FmouseColon_metadata.json'
    r = client.get(f"/s3-resource/{small_s3_file}")

    assert r.status_code == 200
    assert b"proximal colon" in r.data


def test_direct_download_url_large_file(client):
    large_s3_file = '61%2F2%2Ffiles%2Fprimary%2Fsub-44%2Fsam-1%2Fmicroscopy%2Fsub-44sam-1C44-1Slide2p2MT_10x.nd2'
    r = client.get(f"/s3-resource/{large_s3_file}")

    assert r.status_code == 413
    assert 'File too big to download' in r.get_data().decode()
