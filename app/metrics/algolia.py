import logging
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

def get_all_dataset_ids():
  client = init_algolia_client()
  index = client.init_index(INDEX)
  object_ids = []
  for record in index.browse_objects():
    if record['objectID'] is not None:
      object_ids.append(record['objectID'])
  return object_ids
  