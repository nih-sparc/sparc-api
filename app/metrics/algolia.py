from algoliasearch.search_client import SearchClient


def init_algolia_client():
    return SearchClient.create("04WW1V1O0F", "9f55092cb37c3896a8bfc48ef87206d6")


def get_dataset_count(client):
    index = client.init_index("k-core_dev")
    res = index.search("")
    return res["nbHits"]
