from app.metrics.contentful import init_cf_cma_client, get_all_entries
from datetime import datetime, timedelta, timezone

def update_event_entries():
    all_event_entries = get_all_entries("event")
    now = datetime.now()
    for entry in all_event_entries:
      fields_dict = entry.fields()
      sys_dict = entry.sys
      if 'title' in fields_dict and 'start_date' in fields_dict and 'version' in sys_dict:
        title = fields_dict['title']
        start_date = fields_dict['start_date']
        #convert from ISO time format provided by contentful in UTC timezone to naive offset datetime object
        start_date_datetime = datetime.strptime(datetime.fromisoformat(start_date).astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f'), '%Y-%m-%d %H:%M:%S.%f')
        time_from_event_in_seconds = (start_date_datetime - now).total_seconds()
        time_from_event_in_days = time_from_event_in_seconds / 86400
        # in order to maintain the correct event sorting for upcoming (closet first, followed by closest in the future, followed by closest in the past),
        # we cannot simply keep track of the time from the event. Instead we take the inverse of the dates in the future so that they are less than the nearest future dates.
        upcoming_sort_order = 1
        if time_from_event_in_days > 0:
          upcoming_sort_order = 1/time_from_event_in_days
        if time_from_event_in_days < 0:
          upcoming_sort_order = time_from_event_in_days
        version = sys_dict['version']
        if 'Test Event' in title:
          entry.upcoming_sort_order = upcoming_sort_order
          entry.save()
          