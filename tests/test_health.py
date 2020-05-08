# Third-party imports...
from nose.tools import assert_equals
import json

# Local imports...
from app.main import health

def test_request_response():
    response = health()
    json_response = json.loads(response)
    assert_equals("healthy", json_response.get("status"))