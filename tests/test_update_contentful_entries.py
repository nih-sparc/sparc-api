from datetime import datetime, timezone, timedelta
from scripts.update_contentful_entries import calculate_sort_order

def test_calculate_sort_order():
    now = datetime.now().astimezone(timezone.utc)
    # Format the date in ISO format
    current_date_iso = now.isoformat()
    # Subtract one day to get a day in the past
    past_date = now - timedelta(days=1)
    past_date_iso = past_date.isoformat()
    # Add one day to get a day in the future
    future_date = now + timedelta(days=1)
    future_date_iso = future_date.isoformat()
    print(f"NOW UTC = ", now)
    print(f"NOW ISO = {current_date_iso}")
    print(f"PAST_DATE = {past_date_iso}")
    print(f"FUTURE_DATE = {future_date_iso}")

    assert calculate_sort_order(past_date_iso) < 0 
    assert calculate_sort_order(future_date_iso) > 0
    assert calculate_sort_order(current_date_iso) == 1.1
    assert calculate_sort_order(past_date_iso, future_date_iso) == 1
    assert calculate_sort_order(past_date_iso, past_date_iso) < 0