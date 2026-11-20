"""
One-off CLI utility to re-sync HubSpot contacts into Email Octopus.

Use this to backfill contacts that the /hubspot_webhook endpoint dropped (e.g. because
HubSpot rate-limited the sync or a signature mismatch rejected the whole batch during a
bulk import). It searches HubSpot for contacts matching a property/value filter, then
pushes each one to the Email Octopus master list, retrying with backoff on 429s.

This intentionally avoids importing anything under app/ -- importing app.config pulls in
app/__init__.py, which imports app.main and would start the Flask app's background
schedulers and other third-party clients as a side effect. This script only needs the
HubSpot/Email Octopus env vars and HTTP calls.

Required env vars: HUBSPOT_API_TOKEN, EMAIL_OCTOPUS_API_KEY, EMAIL_OCTOPUS_MASTER_LIST_ID.

Usage:
    python -m scripts.resync_hubspot_emailoctopus --value SfN2026
    python -m scripts.resync_hubspot_emailoctopus --property event_name --value SfN2026 --dry-run
"""
import argparse
import logging
import os
import random
import sys
import time

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

HUBSPOT_API_TOKEN = os.environ.get("HUBSPOT_API_TOKEN")
EMAIL_OCTOPUS_API_KEY = os.environ.get("EMAIL_OCTOPUS_API_KEY")
EMAIL_OCTOPUS_MASTER_LIST_ID = os.environ.get("EMAIL_OCTOPUS_MASTER_LIST_ID")
HUBSPOT_V3_API = os.environ.get("HUBSPOT_V3_API", "https://api.hubapi.com/crm/v3")

HUBSPOT_SEARCH_URL = f"{HUBSPOT_V3_API}/objects/contacts/search"
CONTACT_PROPERTIES = ["firstname", "lastname", "email", "newsletter", "event_name"]


def get_retry_delay_seconds(headers, attempt, base_delay=1.0, max_delay=30.0):
    retry_after = headers.get("Retry-After") if headers else None
    if retry_after:
        try:
            return float(retry_after)
        except (TypeError, ValueError):
            pass
    return min(base_delay * (2 ** attempt), max_delay) + random.uniform(0, 0.5)


def request_with_retry(method, url, max_retries=5, **kwargs):
    response = None
    for attempt in range(max_retries + 1):
        response = requests.request(method, url, **kwargs)
        if response.status_code == 429 and attempt < max_retries:
            delay = get_retry_delay_seconds(response.headers, attempt)
            logging.warning(f"429 from {url}, retrying in {delay:.1f}s (attempt {attempt + 1}/{max_retries})")
            time.sleep(delay)
            continue
        return response
    return response


def find_hubspot_contacts(property_name, value):
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + HUBSPOT_API_TOKEN,
    }
    after = None
    while True:
        payload = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": property_name,
                    "operator": "CONTAINS_TOKEN",
                    "value": value,
                }]
            }],
            "properties": CONTACT_PROPERTIES,
            "limit": 100,
        }
        if after:
            payload["after"] = after
        response = request_with_retry("POST", HUBSPOT_SEARCH_URL, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        for result in data.get("results", []):
            yield result
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break
        # stay well under HubSpot's burst limit between search pages
        time.sleep(0.2)


def sync_contact_to_emailoctopus(contact, dry_run=False):
    properties = contact.get("properties", {})
    email = properties.get("email")
    if not email:
        logging.warning(f"Skipping contact {contact.get('id')} with no email")
        return False
    firstname = properties.get("firstname") or ""
    lastname = properties.get("lastname") or ""
    newsletter_tags = [t for t in (properties.get("newsletter") or "").split(";") if t]
    event_tags = [t for t in (properties.get("event_name") or "").split(";") if t]
    tags = newsletter_tags + event_tags

    if dry_run:
        logging.info(f"[dry-run] Would sync {email} with tags {tags}")
        return True

    url = f"https://api.emailoctopus.com/lists/{EMAIL_OCTOPUS_MASTER_LIST_ID}/contacts"
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + EMAIL_OCTOPUS_API_KEY,
    }
    payload = {
        "email_address": email,
        "fields": {"FirstName": firstname, "LastName": lastname},
        "status": "subscribed",
        # Email Octopus expects tags as an object of {tagName: true/false}, not an array.
        "tags": {tag: True for tag in tags},
    }
    response = request_with_retry("PUT", url, headers=headers, json=payload)
    if response.status_code != 200:
        logging.error(f"Failed to sync {email}: {response.status_code} {response.text}")
        return False
    logging.info(f"Synced {email} with tags {tags}")
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--property", default="event_name", help="HubSpot contact property to filter on")
    parser.add_argument("--value", required=True, help="Value the property must contain, e.g. SfN2026")
    parser.add_argument("--dry-run", action="store_true", help="Only print what would be synced, without writing to Email Octopus")
    parser.add_argument("--delay", type=float, default=0.2, help="Seconds to sleep between Email Octopus writes")
    args = parser.parse_args()

    missing = [name for name, value in (
        ("HUBSPOT_API_TOKEN", HUBSPOT_API_TOKEN),
        ("EMAIL_OCTOPUS_API_KEY", EMAIL_OCTOPUS_API_KEY),
        ("EMAIL_OCTOPUS_MASTER_LIST_ID", EMAIL_OCTOPUS_MASTER_LIST_ID),
    ) if not value]
    if missing:
        logging.error(f"Missing required environment variable(s): {', '.join(missing)}")
        sys.exit(1)

    total = 0
    succeeded = 0
    for contact in find_hubspot_contacts(args.property, args.value):
        total += 1
        if sync_contact_to_emailoctopus(contact, dry_run=args.dry_run):
            succeeded += 1
        time.sleep(args.delay)

    logging.info(f"Done. {succeeded}/{total} contacts synced successfully.")


if __name__ == "__main__":
    main()
