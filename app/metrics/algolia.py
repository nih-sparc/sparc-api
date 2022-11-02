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
