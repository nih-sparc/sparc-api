import logging
from datetime import datetime

from app.config import Config
from dateutil.relativedelta import relativedelta
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

SCOPE = Config.GOOGLE_API_GA_SCOPE
KEY_PATH = Config.GOOGLE_API_GA_KEY_PATH
VIEW_ID = Config.GOOGLE_API_GA_VIEW_ID


def init_ga_reporting():
    try:
        if KEY_PATH:
            credentials = ServiceAccountCredentials.from_json_keyfile_name(
                KEY_PATH,
                SCOPE
            )
            analytics = build('analyticsreporting', 'v4', credentials=credentials)
            return analytics
        else:
            logging.info('No key path set for Google analytics.')
            return None

    except TypeError as e:
        logging.error('An error occurred while instantiating the GA reporter.', str(e))
        return None


def get_ga_1year_sessions(analytics):

    start_date = datetime.now() - relativedelta(years=1)
    formatted_start_date = start_date.strftime('%Y-%m-%d')

    try:
        report = analytics.reports().batchGet(
            body={
                "reportRequests": [{
                    "viewId": VIEW_ID,
                    "dateRanges": [{
                        "startDate": formatted_start_date,
                        "endDate": datetime.now().strftime('%Y-%m-%d')
                    }],
                    "metrics": [{"expression": "ga:sessions"}]
                }]
            }
        ).execute()

        if len(report["reports"]):
            total = report["reports"][0]["data"]["totals"][0]["values"][0]
            return int(total)

    except:
        return None
