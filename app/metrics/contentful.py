import logging
from app.config import Config
import contentful
import contentful_management
import requests

#CDA is used for reading content, while CMA is used for updating content
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

def get_homepage_response(client):
  response = client.entry(Config.CTF_HOMEPAGE_ID)
  return response.fields()

def get_all_entries(content_type_id):
  client = init_cf_cma_client()
  content_type = client.content_types(SPACE_ID, 'master').find(content_type_id)
  return content_type.entries().all()

def get_all_published_entries(content_type_id):
    url = f'https://{Config.CTF_CMA_API_HOST}/spaces/{Config.CTF_SPACE_ID}/environments/master/public/entries?access_token={Config.CTF_CMA_ACCESS_TOKEN}&content_type={content_type_id}'
    response = requests.get(url)
    json_response = response.json()
    return json_response['items']
    
