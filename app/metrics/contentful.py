import logging

from app.config import Config

import contentful

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
        client = contentful.Client(
            SPACE_ID,
            CMA_ACCESS_TOKEN,
            api_url=CMA_API_HOST
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

def get_all_entries(content_type):
  client = init_cf_cda_client()
  all_entries = []
  # max limit contentful provides is 1000
  query = { 'limit': '10', 'skip': 0, "content_type": content_type }
  total_entries = client.entries({
    "content_type": content_type
  }).total
  for i in range((total_entries / 10) + 1):
    query['skip'] = i * 10
    page_response = client.entries(query)
    for item in page_response.items:
      all_entries.append(item)
  return all_entries
