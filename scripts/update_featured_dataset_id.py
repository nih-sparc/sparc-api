from app.config import Config
from app.metrics.contentful import get_cda_client_entry, get_cma_entry, update_entry_using_json_response, publish_entry, get_cma_published_entry
from app.metrics.algolia import get_all_dataset_ids

from datetime import datetime, timedelta
import json
import logging
import random

def set_featured_dataset_id(featuredDatasetIdSelectorTable):
    try:
        # only use the dev server to clear featured datasets data in contentful. If we handled updating contentful via both dev and prod,
        # we might run into concurrency issues when updating the homepage
        if Config.DEPLOY_ENV == 'development' and Config.SPARC_API_DEBUGGING == 'FALSE':
            homepage_cma_staging_entry = get_cma_entry(Config.CTF_HOMEPAGE_ID)
            homepage_cma_published_entry = get_cma_published_entry(Config.CTF_HOMEPAGE_ID)
            if 'dateToClearFeaturedDatasets' in homepage_cma_published_entry['fields']:
              date_to_clear_datasets = homepage_cma_published_entry['fields']['dateToClearFeaturedDatasets']['en-US']
              if date_to_clear_datasets is not None:
                  datetime_date_to_clear_datasets = datetime.strptime(date_to_clear_datasets, '%Y-%m-%d')
                  if (datetime_date_to_clear_datasets - datetime.now()).total_seconds() <= 0:
                      # Clear featured datasets/date and re-publish homepage
                      if 'featuredDatasets' in homepage_cma_published_entry['fields']:
                          homepage_cma_published_entry['fields']['featuredDatasets']['en-US'] = []
                      homepage_cma_published_entry['fields']['dateToClearFeaturedDatasets']['en-US'] = None
                      # set the staging state as well so that staging reflects prod
                      if 'featuredDatasets' in homepage_cma_staging_entry['fields']:
                          homepage_cma_staging_entry['fields']['featuredDatasets']['en-US'] = []
                      if 'dateToClearFeaturedDatasets' in homepage_cma_published_entry['fields']:
                          homepage_cma_staging_entry['fields']['dateToClearFeaturedDatasets']['en-US'] = None
                      updated_published_state = {
                          'fields': homepage_cma_published_entry['fields'],
                          'metadata': homepage_cma_published_entry['metadata']
                      }
                      updated_entry = update_entry_using_json_response('homepage', Config.CTF_HOMEPAGE_ID, updated_published_state)
                      publish_entry(Config.CTF_HOMEPAGE_ID, updated_entry['sys']['version'])
                      if 'publishedAt' in homepage_cma_staging_entry['sys']:
                        # convert UTC time strings into datetime objects
                        homepage_staging_updated_at = datetime.strptime(homepage_cma_staging_entry['sys']['updatedAt'], '%Y-%m-%dT%H:%M:%S.%fZ')
                        homepage_staging_published_at = datetime.strptime(homepage_cma_staging_entry['sys']['publishedAt'], '%Y-%m-%dT%H:%M:%S.%fZ')
                        if (homepage_staging_updated_at - homepage_staging_published_at).total_seconds() > 0:
                            # update back to original state if there were existing changes
                            original_state = {
                                'fields': homepage_cma_staging_entry['fields'],
                                'metadata': homepage_cma_staging_entry['metadata']
                            }
                            update_entry_using_json_response('homepage', Config.CTF_HOMEPAGE_ID, original_state)
        # we can update the table state independently for each environment since dev and prod have seperate DB's
        logging.info('Setting featured dataset id selector state info')
        if featuredDatasetIdSelectorTable is None:
            return
        table_state = get_featured_dataset_id_table_state(featuredDatasetIdSelectorTable)   
        cf_homepage_response = get_cda_client_entry(Config.CTF_HOMEPAGE_ID).fields()
        limited_ids_were_set = set_limited_dataset_ids(featuredDatasetIdSelectorTable, table_state, cf_homepage_response)
        if (limited_ids_were_set):
            table_state = get_featured_dataset_id_table_state(featuredDatasetIdSelectorTable)   

        last_used_time = datetime.strptime(table_state["last_used_time"], '%Y-%m-%d %H:%M:%S.%f')
        featured_dataset_id = int(table_state["featured_dataset_id"])
        try:
            time_delta_in_hours = cf_homepage_response['time_delta']
        except Exception:
            time_delta_in_hours = 8
        time_delta_in_days = float(time_delta_in_hours) / 24
        now = datetime.now()
        # If running in a window of time that is shorter than the time delta set in contentful and the limited available ids was not just set then return the same id, otherwise update the id
        if (now - last_used_time) < timedelta(days=time_delta_in_days) and featured_dataset_id != -1 and limited_ids_were_set is False:
            return featured_dataset_id
        # reset the list of ids if we have iterated through all of them already or if the limited available ids list was just set
        if len(table_state["available_dataset_ids"]) == 0 or limited_ids_were_set is True:
            if (len(table_state["limited_available_ids"]) > 0):
                table_state["available_dataset_ids"] = table_state["limited_available_ids"].copy()
            else:
                table_state["available_dataset_ids"] = get_all_dataset_ids()
        available_dataset_ids_array = table_state["available_dataset_ids"]
        random_index = random.randint(0, len(available_dataset_ids_array)-1)
        table_state["featured_dataset_id"] = available_dataset_ids_array.pop(random_index)
        table_state["last_used_time"] = now.strftime('%Y-%m-%d %H:%M:%S.%f')
        table_state["available_dataset_ids"] = available_dataset_ids_array
        featuredDatasetIdSelectorTable.updateState(Config.FEATURED_DATASET_ID_SELECTOR_TABLENAME, json.dumps(table_state), True)
    except Exception as e:
        print('Error while setting featured dataset id: ', e)

def set_limited_dataset_ids(table, table_state, contentful_state):
    persisted_limited_available_ids = table_state["limited_available_ids"]
    try:
        updated_limited_available_ids = contentful_state['featured_datasets']
    except Exception:
        updated_limited_available_ids = []

    # If setting to the same values (regardless of order and duplicates) then do nothing
    if (set(persisted_limited_available_ids) == set(updated_limited_available_ids)):
        return False
    else:
        table_state["limited_available_ids"] = updated_limited_available_ids
        table.updateState(Config.FEATURED_DATASET_ID_SELECTOR_TABLENAME, json.dumps(table_state), True)
        return True

def get_featured_dataset_id_table_state(table):
    default_data = {
      'last_used_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
      'available_dataset_ids': [],
      # limited_available_ids are used if a subset of ids is to be used for featured dataset id selection as opposed to all id's
      'limited_available_ids': [],
      'featured_dataset_id': -1,
    }
    if table is None:
        return default_data
    try:
        current_state = table.pullState(Config.FEATURED_DATASET_ID_SELECTOR_TABLENAME)
        if current_state is None:
            current_state = table.updateState(Config.FEATURED_DATASET_ID_SELECTOR_TABLENAME, json.dumps(default_data), True)
        return json.loads(current_state)
    except:
        return default_data