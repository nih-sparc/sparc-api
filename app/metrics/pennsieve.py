from datetime import datetime

import requests
from app.config import Config
from dateutil.relativedelta import relativedelta

API_URL = Config.DISCOVER_API_HOST

def get_download_count():

    start_date = datetime.now() - relativedelta(years=1)
    formatted_start_date = start_date.strftime('%Y-%m-%d')

    params = {
        "startDate": formatted_start_date,
        "endDate": datetime.now().strftime('%Y-%m-%d')
    }
    req = requests.get(API_URL + '/metrics/dataset/downloads/summary', params=params)

    if (req):
        resp = req.json()

        count = 0
        for dataset in resp:
            count += dataset["downloads"]

        return count

    return None
