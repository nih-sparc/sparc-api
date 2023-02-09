from app.metrics.contentful import get_all_entries, get_all_published_entries, update_entry_using_json_response, get_client_entry, publish_entry
from datetime import datetime, timezone

def update_event_entries():
    all_event_entries = get_all_entries("event")
    all_published_event_entries = get_all_published_entries("event")
    # Create dict with id's as the key so we do not have to iterate through each time we publish an entry
    published_event_id_to_fields_mapping = {}
    for published_event in all_published_event_entries:
        published_event_id = published_event['sys']['id']
        published_event_id_to_fields_mapping[published_event_id] = published_event
    for entry in all_event_entries:
        original_fields_dict = entry['fields']
        original_metadata_dict = entry['metadata']
        if 'startDate' in original_fields_dict and 'upcomingSortOrder' in original_fields_dict and entry['sys']['id']:
            entry_id = entry['sys']['id']
            client_entry = get_client_entry(entry_id)
            entry_had_existing_changes = client_entry.is_updated
            entry_is_published = client_entry.is_published
            start_date = original_fields_dict['startDate']['en-US']

            # convert from ISO time format provided by contentful in UTC timezone to naive offset datetime object
            start_date_datetime = datetime.strptime(datetime.fromisoformat(start_date).astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f'), '%Y-%m-%d %H:%M:%S.%f')
            time_from_event_in_seconds = (start_date_datetime - datetime.now()).total_seconds()
            time_from_event_in_days = time_from_event_in_seconds / 86400
            # in order to maintain the correct event sorting for upcoming (closet first, followed by closest in the future, followed by closest in the past),
            # we cannot simply keep track of the time from the event. Instead we take the inverse of the dates in the future so that they are less than the nearest future dates.
            upcoming_sort_order = 1
            if time_from_event_in_days > 0:
                upcoming_sort_order = 1/time_from_event_in_days
            if time_from_event_in_days < 0:
                upcoming_sort_order = time_from_event_in_days
            original_fields_dict['upcomingSortOrder']['en-US'] = upcoming_sort_order
            if entry_is_published:
                # if entry has changes that are not yet published then we want to publish only the already published state
                published_fields_state = published_event_id_to_fields_mapping[entry_id]['fields']
                published_fields_state['upcomingSortOrder']['en-US'] = upcoming_sort_order
                updated_state = {
                    'fields': published_fields_state,
                    'metadata': published_event_id_to_fields_mapping[entry_id]['metadata']
                }
                updated_entry = update_entry_using_json_response('event', entry_id, updated_state).json()
                publish_entry(entry_id, updated_entry['sys']['version'])
                print(f"{original_fields_dict['title']}")
            if entry_had_existing_changes:
                # after publishing, save it again with the pre-existing changes that were already there
                original_state = {
                    'fields': original_fields_dict,
                    'metadata': original_metadata_dict
                }
                update_entry_using_json_response('event', entry_id, original_state).json()

