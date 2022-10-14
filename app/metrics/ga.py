from datetime import datetime

from dateutil.relativedelta import relativedelta
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]
KEY_PATH = "/home/ignapas/workspace/portal/sparc-api/norse-coral-365411-65a40e36f371.json"
VIEW_ID = "175688725"


def init_ga_reporting():
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        KEY_PATH,
        SCOPES
    )
    analytics = build('analyticsreporting', 'v4', credentials=credentials)
    return analytics


def get_ga_1year_sessions(analytics):

    start_date = datetime.now() - relativedelta(years=1)
    formatted_start_date = start_date.strftime('%Y-%m-%d')

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
        return report["reports"][0]["data"]["totals"][0]["values"][0]
