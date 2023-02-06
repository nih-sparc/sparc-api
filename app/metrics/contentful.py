import logging

from app.config import Config

import contentful

API_HOST = Config.CTF_API_HOST
ACCESS_TOKEN = Config.CTF_CDA_ACCESS_TOKEN
SPACE_ID = Config.CTF_SPACE_ID


def init_cf_client():
    try:
        client = contentful.Client(
            SPACE_ID,
            ACCESS_TOKEN,
            api_url=API_HOST
        )
        return client
        
    except Exception as e:
        logging.error('An error occured while instantiating the Contentful client.', e)
        return None


def get_funded_projects_count(client):
    response = client.entries({
        "content_type": "sparcAward"
    })
    return response.total

def get_homepage_response(client):
  response = client.entry(Config.CTF_HOMEPAGE_ID)
  return response.fields()
