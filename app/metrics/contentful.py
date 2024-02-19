import logging
import contentful
import contentful_management
import requests

from datetime import date, timezone

from app.config import Config

# CDA is used for reading content, while CMA is used for updating content
CDA_API_HOST = Config.CTF_CDA_API_HOST
CMA_API_HOST = Config.CTF_CMA_API_HOST
CDA_ACCESS_TOKEN = Config.CTF_CDA_ACCESS_TOKEN
CMA_ACCESS_TOKEN = Config.CTF_CMA_ACCESS_TOKEN
SPACE_ID = Config.CTF_SPACE_ID


def init_cf_cda_client():
    try:
        client = contentful.Client(
            SPACE_ID,
            CDA_ACCESS_TOKEN,
            api_url=CDA_API_HOST
        )
        return client

    except Exception as e:
        logging.error('An error occured while instantiating the Contentful CDA client.', e)
        return None


def init_cf_cma_client():
    try:
        client = contentful_management.Client(
            CMA_ACCESS_TOKEN
        )
        return client

    except Exception as e:
        logging.error('An error occured while instantiating the Contentful CMA client.', e)
        return None


def get_funded_projects_count(client):
    response = client.entries({
        "content_type": "sparcAward"
    })
    return response.total


def get_cda_client_entry(id):
    client = init_cf_cda_client()
    return client.entry(id)


def get_all_entries(content_type_id):
    url = f'https://{Config.CTF_CMA_API_HOST}/spaces/{Config.CTF_SPACE_ID}/environments/master/entries?access_token={Config.CTF_CMA_ACCESS_TOKEN}&content_type={content_type_id}&limit=999'
    response = requests.get(url=url)
    return response.json()['items']


def get_all_published_entries(content_type_id):
    url = f'https://{Config.CTF_CMA_API_HOST}/spaces/{Config.CTF_SPACE_ID}/environments/master/public/entries?access_token={Config.CTF_CMA_ACCESS_TOKEN}&content_type={content_type_id}&limit=999'
    response = requests.get(url)
    return response.json()['items']


def get_cma_published_entry(id):
    url = f'https://{Config.CTF_CMA_API_HOST}/spaces/{Config.CTF_SPACE_ID}/environments/master/public/entries/{id}?access_token={Config.CTF_CMA_ACCESS_TOKEN}'
    response = requests.get(url)
    return response.json()


def get_cma_entry(id):
    url = f'https://{Config.CTF_CMA_API_HOST}/spaces/{Config.CTF_SPACE_ID}/environments/master/entries/{id}?access_token={Config.CTF_CMA_ACCESS_TOKEN}'
    response = requests.get(url=url)
    return response.json()


# Since get_all_published_entries has to use direct HTTP endpoint its response is in a different format than when using the client to get_all_entries
# Therefore, in order to update an entry with that kind of response we must use this method instead of the client SDK update method
def update_entry_using_json_response(content_type, id, data):
    version = get_cma_entry(id)['sys']['version']

    url = f'https://{Config.CTF_CMA_API_HOST}/spaces/{Config.CTF_SPACE_ID}/environments/master/entries/{id}'
    hed = {
        'Authorization': 'Bearer ' + Config.CTF_CMA_ACCESS_TOKEN,
        'Content-Type': 'application/vnd.contentful.management.v1+json',
        'Accept': 'application/json',
        'X-Contentful-Content-Type': str(content_type),
        'X-Contentful-Version': str(version)
    }

    response = requests.put(
        headers=hed,
        url=url,
        json=data
    )
    return response.json()


def publish_entry(id, version):
    url = f'https://{Config.CTF_CMA_API_HOST}/spaces/{Config.CTF_SPACE_ID}/environments/master/entries/{id}/published'
    hed = {
        'Authorization': 'Bearer ' + Config.CTF_CMA_ACCESS_TOKEN,
        'X-Contentful-Version': str(version)
    }

    response = requests.put(
        headers=hed,
        url=url
    )
    return response.json()


def _have_featured_datasets(result):
    if len(result['items'][0]) == 1:
        featured_data = result['items'][0]
        return 'featuredDatasets' in featured_data['fields'] and 'dateToClearFeaturedDatasets' in featured_data['fields']

    return False


def get_featured_datasets():
    url = f'https://{Config.CTF_CDA_API_HOST}/spaces/{Config.CTF_SPACE_ID}/environments/master/entries'
    hed = {
        'Authorization': 'Bearer ' + Config.CTF_CDA_ACCESS_TOKEN,
    }
    q = {
        'content_type': 'homepage',
        'select': 'fields.featuredDatasets,fields.dateToClearFeaturedDatasets',
    }

    response = requests.get(
        headers=hed,
        params=q,
        url=url
    )

    featured_datasets = []
    if response.status_code == requests.codes.ok:
        json_data = response.json()

        if _have_featured_datasets(json_data):
            featured_data = json_data['items'][0]
            date_to_clear = featured_data['fields']['dateToClearFeaturedDatasets']
            featured_datasets = featured_data['fields']['featuredDatasets']
            if date_to_clear is not None:
                time_now = date.today()
                expiration_time = date.fromisoformat(date_to_clear)
                if expiration_time < time_now:
                    featured_datasets = []

    return featured_datasets
