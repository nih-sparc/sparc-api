import datetime
import pytest
import requests
from app.config import Config
from scripts.monthly_stats import MonthlyStats
from nose.tools import assert_true

#  The email address below can be modified to check the emails are sending and look as expected
#  (using any email you control is fine as long as it is not pushed to github)

test_email_recipient = Config.TESTING_EMAIL_ADDRESS

test_data = {
    "0000-0002-3722-6351": {
        "datasets": [
            {
                "datasetId": 230,
                "name": "Test dataset 1",
                "version": 1,
                "origin": "SPARC",
                "downloads": 1
            },
            {
                "datasetId": 225,
                "name": "Test dataset 2",
                "version": 1,
                "origin": "SPARC",
                "downloads": 1
            },
            {
                "datasetId": 141,
                "name": "Test dataset 3",
                "version": 2,
                "origin": "SPARC",
                "downloads": 1
            }
        ],
        "email": test_email_recipient,
        "lastName": "TestUser"
    }
}

ms = MonthlyStats(debug_mode=True, debug_email=test_email_recipient)


def test_pennsieve_login():
    key = ms.pennsieve_login()
    r = requests.get(f"{Config.PENNSIEVE_API_HOST}/datasets",
                     headers={"Authorization": f"Bearer {key}"})
    assert_true(r.status_code == 200)


def test_metrics_endpoint():
    stats = ms.get_download_metrics_one_month()
    assert_true(len(stats) > 0)  # note this assumes there is at least one download a month


def test_stats_generation():
    stats = ms.get_stats()
    assert_true(len(stats.keys()) > 0)

# Note: unfortunately the sendgrid python client returns 202 regardless of whether the email was sent or not.
#   There is also no body to see if the request is successful. Because of this all we will do is check for 202
#   There may be more control calling the sendgrid api directly if we switch to it

def test_email():  # Note that this will send an email to the test_"email_recipient" provided at the top of this doc
    responses = ms.send_stats(test_data)
    assert_true(False not in [r.status_code == 202 for r in responses])  # Check all responses were 202   

def test_full_run():  # For each recipient, this will send an email to the test_email for each email that would have been sent to a user
    #Only check with the DB if it  is set up
    responses = []
    if Config.DATABASE_URL is None:
        responses = ms.run()
    else:
        #Test against a specified date, the entry in the database is set
        #with the latest date being 2024, 7, 1
        #do not provide a date, it will use the current date instead
        responses = ms.monthly_stats_required_check(None, False)
    assert_true(False not in [r.status_code == 202 for r in responses])

def test_next_year_run():  # For each recipient, this will send an email to the test_email for each email that would have been sent to a user
    #Only check with the DB if it  is set up
    if Config.DATABASE_URL is None:
        pytest.skip('DATABASE not set for this run')
    else:
        #Test against a specified date, the entry in the database is set
        #with the latest date being 2024, 7, 1
        timeNow = datetime.datetime(2025, 1, 1).date()
        responses = ms.monthly_stats_required_check(timeNow, False)
        assert_true(False not in [r.status_code == 202 for r in responses])

def test_same_month_run():
    if Config.DATABASE_URL is None:
        pytest.skip('DATABASE not set for this run')
    else:
        #Test against a specified date, the entry in the database is set
        #with the latest date being 2024, 7, 1
        timeNow = datetime.datetime(2024, 7, 4).date()
        responses = ms.monthly_stats_required_check(timeNow, False)
        assert_true(len(responses) == 0)

def test_earlier_month_run():
    if Config.DATABASE_URL is None:
        pytest.skip('DATABASE not set for this run')
    else:
        #Test against a specified date, the entry in the database is set
        #with the latest date being 2024, 7, 1
        timeNow = datetime.datetime(2024, 6, 1).date()
        responses = ms.monthly_stats_required_check(timeNow, False)
        assert_true(len(responses) == 0)

def test_log_email():
    response = ms.send_logging_email('testing log email')
    assert_true(response.status_code == 202)
