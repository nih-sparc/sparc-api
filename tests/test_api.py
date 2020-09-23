import pytest
import json
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

def test_get_datasets_by_project(client):
  # SPARC Portal project info
  portal_project_id = 'OT2OD025340'

  r = client.get(f"/project/{999999}")
  assert r.status_code == 404

  r = client.get(f"/project/{portal_project_id}")
  assert r.status_code == 200

