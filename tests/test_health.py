# Third-party imports...
import json

from nose.tools import assert_equals

# Local imports...
from app.main import health


def test_request_response():
    response = health()
    json_response = json.loads(response)
    assert_equals("healthy", json_response.get("status"))
