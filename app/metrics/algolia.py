from algoliasearch.search_client import SearchClient
from app.config import Config

APP_ID = Config.ALGOLIA_APP_ID
API_KEY = Config.ALGOLIA_API_KEY


def init_algolia_client():
    return SearchClient.create(APP_ID, API_KEY)


def get_dataset_count(client):
    index = client.init_index("k-core_dev")
    res = index.search("")
    return res["nbHits"]
