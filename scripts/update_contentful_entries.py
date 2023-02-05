from app.metrics.contentful import init_cf_cma_client, get_all_entries
from datetime import datetime, timedelta

def update_event_entries():
    all_event_entries = get_all_entries("event")
    now = datetime.now()
    for entry in all_event_entries:
      fields_dict = entry.fields()
      sys_dict = entry.sys
      if 'title' in fields_dict and 'start_date' in fields_dict and 'version' in sys_dict:
        title = fields_dict['title']
        start_date = fields_dict['start_date']
        timeFromEventInSeconds = (start_date - now).total_seconds()
        timeFromEventInDays = timeFromEventInSeconds / 86400
        version = sys_dict['version']
        if 'Test Event' in title:
          entry.title = 'Updated Title'
          entry.save()

        #start_date = entry.fields()['start_date']
        #version = entry.sys
        #print("For {}: Start date = {}, Version = {}", title, start_date, version)
        print("Title = ", title)
        print("Start Date = ", start_date)
        print("Time from event = ", timeFromEventInDays)