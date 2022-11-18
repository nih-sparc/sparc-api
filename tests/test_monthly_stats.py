import requests
from app.config import Config
from scripts.monthly_stats import MonthlyStats
from nose.tools import assert_true

test_email_recipient = 'myname@domain.com'

test_data = {
    "0000-0002-3722-6351": {
        "datasets": [
            {
                "datasetId": 230,
                "version": 1,
                "origin": "SPARC",
                "downloads": 1
            },
            {
                "datasetId": 225,
                "version": 1,
                "origin": "SPARC",
                "downloads": 1
            },
            {
                "datasetId": 141,
                "version": 2,
                "origin": "SPARC",
                "downloads": 1
            }
        ],
        "email": test_email_recipient
    }
}

ms = MonthlyStats()

def test_pennsieve_login():
    key = ms.pennsieve_login()
    r = requests.get(f"{Config.PENNSIEVE_API_HOST}/datasets",
                     headers={"Authorization": f"Bearer {key}"})
    assert_true(r.status_code == 200)

def test_metrics_endpoint():
    stats = ms.get_download_metrics_one_month()
    assert_true(len(stats.keys()) > 0) # note this assumes there is at least one download a month

def test_stats_generation():
    stats = ms.get_stats()
    assert_true(len(stats.keys()) > 0)

def test_email(): # Note that this will send an email to the test_"email_recipient" provided at the top of this doc
    response = ms.send_stats(test_data)
    assert_true(response.status_code == 200)