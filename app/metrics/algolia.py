import logging
import numpy
from algoliasearch.search_client import SearchClient
from app.config import Config

APP_ID = Config.ALGOLIA_APP_ID
API_KEY = Config.ALGOLIA_API_KEY
INDEX = Config.ALGOLIA_INDEX


def init_algolia_client():
    try:
        return SearchClient.create(APP_ID, API_KEY)
        
    except Exception as e:
        logging.error('An error occured while instantiating the Algolia search client.', e)
        return None


def get_dataset_count(client):
    index = client.init_index(INDEX)
    res = index.search("")
    return res["nbHits"]

def get_all_dataset_ids(client):
  index = client.init_index(INDEX)
  res = index.search("")
  hits = res["hits"]
  # if for some reason a dataset does not have an id then assign it -1 and we filter it out
  object_ids = numpy.array([getattr(hit, 'objectID', '-1') for hit in hits])
  filter_invalid_ids = object_ids >= 0
  return object_ids[filter_invalid_ids]