from json import JSONDecodeError
import logging
from datetime import datetime

from app.config import Config
from dateutil.relativedelta import relativedelta
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from oauth2client.service_account import ServiceAccountCredentials

import io

SCOPE = Config.GOOGLE_API_GA_SCOPE
SPREADS_SCOPE = Config.GOOGLE_API_SPREADS_SCOPE
DRIVE_SCOPE = Config.GOOGLE_API_DRIVE_SCOPE
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
            logging.info('No key path set for Google JSON credential file.')
            return None

    except JSONDecodeError as e:
        logging.error('An error occurred while instantiating the GA reporter.', str(e))
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

def init_gspread_client():
    try:
        if KEY_PATH:
            credentials = ServiceAccountCredentials.from_json_keyfile_name(
                KEY_PATH,
                SPREADS_SCOPE
            )
            sheets = build('sheets', 'v4', credentials=credentials)
            return sheets
        else:
            logging.info('No key path set for Google JSON credential file.')
            return None

    except JSONDecodeError as e:
        logging.error('An error occurred while instantiating the GSPREAD client.', str(e))
        return None
    except TypeError as e:
        logging.error('An error occurred while instantiating the GSPREAD client.', str(e))
        return None
    
def append_contact(client, row):
    if client:
        client.spreadsheets().values().append(
            spreadsheetId='1SmgquFqUZLsBty5CJBNkRGCmXj_OQiY-dT6WN9L684c',
            range='Events',
            valueInputOption='USER_ENTERED',
            insertDataOption='INSERT_ROWS',
            body={
                'values': [row]
            }
        ).execute()
    return True

def init_drive_client():
    try:
        if KEY_PATH:
            credentials = ServiceAccountCredentials.from_json_keyfile_name(
                KEY_PATH,
                DRIVE_SCOPE
            )
            drive = build('drive', 'v3', credentials=credentials)
            return drive
        else:
            logging.info('No key path set for Google JSON credential file.')
            return None

    except JSONDecodeError as e:
        logging.error('An error occurred while instantiating the DRIVE client.', str(e))
        return None
    except TypeError as e:
        logging.error('An error occurred while instantiating the DRIVE client.', str(e))
        return None
    
def upload_file(client, file, filename):
    if file.filename == '':
        return False
    file_stream = io.BytesIO(file.read())
    mime_type = file.mimetype or 'application/octet-stream'
    file_metadata = {
        'name': filename or file.filename,
        'parents': ['1WO7wZdamhZYse_MjPaHzqZQ7FMlHsf15']
    }
    media = MediaIoBaseUpload(file_stream, mimetype=mime_type, resumable=True)
    resp = client.files().create(
        body=file_metadata,
        media_body=media,
        fields='webViewLink',
        supportsAllDrives=True
    ).execute()
    return resp
