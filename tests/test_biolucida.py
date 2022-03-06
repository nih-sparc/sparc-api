import pytest
import unittest

from app.main import authenticate_biolucida, thumbnail_by_image_id, image_info_by_image_id
from app.main import Biolucida

from app import app


@pytest.fixture
def client():
    # Spin up test flask app
    app.config['TESTING'] = True
    return app.test_client()


class BiolucidaTestCase(unittest.TestCase):

    def test_authenticate(self):
        bl = Biolucida()
        authenticate_biolucida()
        self.assertNotEqual('', bl.token())

    def test_get_image_info(self):
        image_info = image_info_by_image_id(1170)
        self.assertEqual('success', image_info['status'])
        self.assertEqual('115', image_info['collection_id'])

    def test_get_thumbnail(self):
        thumbnail = thumbnail_by_image_id(1170)
        self.assertTrue(thumbnail.startswith(b'/9j/4AAQSkZJRgABAQAAAQ'))

    def test_old_token(self):
        bl = Biolucida()
        bl.set_token('a20f155e818fbfebbb03275f30f87697')
        thumbnail = thumbnail_by_image_id(1170)
        self.assertTrue(thumbnail.startswith(b'/9j/4AAQSkZJRgABAQAAAQ'))

    def test_bad_token(self):
        bl = Biolucida()
        bl.set_token('bad_token')
        thumbnail = thumbnail_by_image_id(1170)
        self.assertTrue(thumbnail.startswith(b'/9j/4AAQSkZJRgABAQAAAQ'))


def test_image_xmp_info_2727(client):
    r = client.get('/image_xmp_info/2727')

    assert 'pixel_width' in r.json
    assert 'channel_colours' in r.json
    assert r.json['pixel_width'] == "0.415133"
    assert r.json['pixel_height'] == "0.415133"
    assert r.json['z_spacing'] == "1.000000"


def test_image_xmp_info_1197(client):
    r = client.get('/image_xmp_info/1197')

    assert 'pixel_width' in r.json
    assert r.json['pixel_width'] == "0.008184"
    assert r.json['pixel_height'] == "0.008184"
    assert r.json['z_spacing'] == "-1.000000"


if __name__ == '__main__':
    unittest.main()
