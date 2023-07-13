from app.metrics.contentful import get_all_entries, get_all_published_entries, update_entry_using_json_response, publish_entry
from datetime import datetime, timezone

def update_event_sort_order(event):
    event_entry_id = event['sys']['id']
    event_entry_fields = event['fields']
    event_entry_metadata = event['metadata']
    if 'startDate' in event_entry_fields and 'upcomingSortOrder' in event_entry_fields:
        start_date = event_entry_fields['startDate']['en-US']
        upcoming_sort_order = calculate_sort_order(start_date)
        event_entry_fields['upcomingSortOrder']['en-US'] = upcoming_sort_order
        event_state = {
            'fields': event_entry_fields,
            'metadata': event_entry_metadata
        }
        return update_entry_using_json_response('event', event_entry_id, event_state)
    else:
        return event

def update_all_events_sort_order():
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
            entry_had_existing_changes = False
            entry_is_published = False 
            if 'publishedAt' in entry['sys']:
                entry_is_published = True
                # convert UTC time strings into datetime objects
                entry_updated_at = datetime.strptime(entry['sys']['updatedAt'], '%Y-%m-%dT%H:%M:%S.%fZ')
                entry_published_at = datetime.strptime(entry['sys']['publishedAt'], '%Y-%m-%dT%H:%M:%S.%fZ')
                if (entry_updated_at - entry_published_at).total_seconds() > 0:
                    entry_had_existing_changes = True
            
            start_date = original_fields_dict['startDate']['en-US']
            upcoming_sort_order = calculate_sort_order(start_date)
            original_fields_dict['upcomingSortOrder']['en-US'] = upcoming_sort_order
            if entry_is_published:
                # if entry has changes that are not yet published then we want to publish only the already published state
                published_fields_state = published_event_id_to_fields_mapping[entry_id]['fields']
                published_fields_state['upcomingSortOrder']['en-US'] = upcoming_sort_order
                updated_state = {
                    'fields': published_fields_state,
                    'metadata': published_event_id_to_fields_mapping[entry_id]['metadata']
                }
                updated_entry = update_entry_using_json_response('event', entry_id, updated_state)
                publish_entry(entry_id, updated_entry['sys']['version'])
            # after publishing, update it again with the pre-existing changes that were already there. Or if it is a draft then just update it in order to set the sort order
            if entry_had_existing_changes or not entry_is_published:
                original_state = {
                    'fields': original_fields_dict,
                    'metadata': original_metadata_dict
                }
                update_entry_using_json_response('event', entry_id, original_state)

def calculate_sort_order(start_date, end_date=None):
    # convert from ISO time format provided by contentful in UTC timezone
    start_date_datetime = datetime.fromisoformat(start_date).astimezone(timezone.utc)
    now = datetime.now().astimezone(timezone.utc)
    time_from_event_in_seconds = (start_date_datetime - now).total_seconds()
    time_from_event_in_days = int(time_from_event_in_seconds / 86400)
    # in order to maintain the correct event sorting for upcoming (closet first, followed by closest in the future, followed by closest in the past),
    # we cannot simply keep track of the time from the event. Instead we take the inverse of the dates in the future so that they are less than the nearest future dates.
    upcoming_sort_order = 1.1
    # if start date is in the future
    if time_from_event_in_days > 0:
        upcoming_sort_order = 1/time_from_event_in_days
    # is start date has passed
    if time_from_event_in_days < 0:
        if end_date is None:
            upcoming_sort_order = time_from_event_in_days
        else:
            end_date_datetime = datetime.fromisoformat(end_date).astimezone(timezone.utc)
            end_time_from_now_in_seconds = (end_date_datetime - now).total_seconds()
            end_time_from_now_in_days = int(end_time_from_now_in_seconds / 86400)
            # if the event is ongoing (meaning its end date has not yet passed)
            has_event_ended = end_time_from_now_in_days < 0
            if not has_event_ended:
                # show ongoing events first
                upcoming_sort_order = 1
            else:
                upcoming_sort_order = time_from_event_in_days

    return upcoming_sort_order
