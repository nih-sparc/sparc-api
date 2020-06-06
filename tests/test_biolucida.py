import unittest

from app.main import authenticate_biolucida, thumbnail_by_image_id, image_info_by_image_id
from app.main import Biolucida


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


if __name__ == '__main__':
    unittest.main()