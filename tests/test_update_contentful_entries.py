from datetime import datetime, timezone, timedelta
from scripts.update_contentful_entries import calculate_sort_order

def test_calculate_sort_order():
    # Get the current UTC date and time
    now = datetime.now(timezone.utc)    
    # Format the date in ISO format
    current_date_iso = now.date().isoformat()
    # Subtract one day to get a day in the past
    past_date = now - timedelta(days=1)
    past_date_iso = past_date.date().isoformat()
    # Add one day to get a day in the future
    future_date = now + timedelta(days=1)
    future_date_iso = future_date.date().isoformat()

    assert calculate_sort_order(past_date_iso) < 0 
    assert calculate_sort_order(future_date_iso) > 0
    assert calculate_sort_order(current_date_iso) == 1.1
    assert calculate_sort_order(past_date_iso, future_date_iso) == 1
    assert calculate_sort_order(past_date_iso, past_date_iso) < 0