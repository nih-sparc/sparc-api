from app.metrics.contentful import init_cf_cma_client, get_all_entries
from datetime import datetime, timedelta

def update_event_entries():
    all_event_entries = get_all_entries("event")
    now = datetime.now()
    for entry in all_event_entries:
      if hasattr(entry.fields(), 'title'):
        title = entry.fields()['title']
        #start_date = entry.fields()['start_date']
        #version = entry.sys
        #print("For {}: Start date = {}, Version = {}", title, start_date, version)
        print("Title = ", title)