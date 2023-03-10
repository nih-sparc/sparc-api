import logging
import boto3
from app.config import Config
from app.metrics.pennsieve import get_pennseive_download_metrics
from scripts.monthly_downloads_html_template import create_html_template
from scripts.email_sender import EmailSender
import requests
import datetime
import json
from dateutil.relativedelta import relativedelta

def remove_duplicates(d_array):
    json_list = [json.dumps(d) for d in d_array]
    json_set = set(json_list)
    unique_list = [json.loads(d) for d in json_set]
    return unique_list

class MonthlyStats(object):
    def __init__(self, debug_mode=False, debug_email=''):
        self.send_grid = EmailSender()
        self.user_stats = {}
        self.organization = Config.PENNSIEVE_ORGANIZATION
        self._pennsieve_temp_api_key = ''
        self.created_at = datetime.datetime.now()
        self.run_day = 1  # This is the day of the month emails will be sent
        self.debug_email = debug_email
        self.debug_mode = debug_mode

    # daily_run_check runs on a set day of the month. However, since we do nt want to send two emails to users if the
    #       app restarts on the first day of the month, We assume an email has already been sent if the app has just
    #       started and set a cooldown period of 24h. This ensures we pass the 1 day trigger for sending the emails
    def daily_run_check(self):
        now = datetime.datetime.now()
        if now.day == self.run_day:  # Check if current day is run day
            if (now - self.created_at) > datetime.timedelta(days=1):  # Do not run if app was started within 24h
                self.run()
            else:
                logging.info('SPARC api has started in the last 24 hours. Waiting until 24h has passed before '
                             'sending emails')

    def run(self):
        self.get_stats()
        return self.send_stats(self.user_stats)

    def get_stats(self):
        self._pennsieve_temp_api_key = self.pennsieve_login()
        metrics = self.get_download_metrics_one_month()
        dataset_details_for_downloaded_datasets = self.get_dataset_details_from_pennsieve(metrics)
        self.user_stats = self.create_user_download_object(dataset_details_for_downloaded_datasets, metrics)
        self.pennsieve_user_details = self.get_emails_orcid_id_map_from_pennsieve()
        self.add_emails_to_user_stats_object()
        return self.user_stats

    def send_stats(self, user_stats):
        responses = []
        for orcid_id in user_stats:
            if 'email' in user_stats[orcid_id].keys():
                email_address = user_stats[orcid_id]['email']
                email_body = create_html_template(remove_duplicates(user_stats[orcid_id]['datasets']))
                r = self.send_email(email_address, email_body)
            responses.append(r)
        return responses

    # Get 1 month's metrics from Pennsieve
    def get_download_metrics_one_month(self):
        return get_pennseive_download_metrics(relativedelta(months=1))

    # Returns pennsieve api token valid for 24 hours
    def pennsieve_login(self):
        r = requests.get(f"{Config.PENNSIEVE_API_HOST}/authentication/cognito-config")
        r.raise_for_status()

        cognito_app_client_id = r.json()["tokenPool"]["appClientId"]
        cognito_region = r.json()["region"]

        cognito_idp_client = boto3.client(
            "cognito-idp",
            region_name=cognito_region,
            aws_access_key_id="",
            aws_secret_access_key="",
        )

        login_response = cognito_idp_client.initiate_auth(
            AuthFlow="USER_PASSWORD_AUTH",
            AuthParameters={"USERNAME": Config.PENNSIEVE_API_TOKEN, "PASSWORD": Config.PENNSIEVE_API_SECRET},
            ClientId=cognito_app_client_id,
        )

        api_key = login_response["AuthenticationResult"]["AccessToken"]
        return api_key

    # Places emails on the an object with orcid_ids
    def get_emails_orcid_id_map_from_pennsieve(self):
        r = requests.get(f"{Config.PENNSIEVE_API_HOST}/organizations/{self.organization}/members",
                         headers={"Authorization": f"Bearer {self._pennsieve_temp_api_key}"})
        r.raise_for_status()
        return r.json()

    # Add an emails field to the user stats object (which has a highest level of orcid id)
    def add_emails_to_user_stats_object(self):
        for user in self.pennsieve_user_details:
            if 'orcid' in user.keys():
                orcid_id = user['orcid']['orcid']
                if orcid_id in self.user_stats.keys():
                    self.user_stats[orcid_id]['email'] = user['email']

    # Get details for a given metrics object
    def get_dataset_details_from_pennsieve(self, metrics):
        # send a request asking for info on the datsets with downloads
        r = requests.get(f'{Config.PENNSIEVE_API_HOST}/discover/datasets', {
            'limit': 1000,
            'ids': [d['datasetId'] for d in metrics]
        })
        r.raise_for_status()
        return r.json()['datasets']

    #  Creates dictionary keyed by orcid id with download stats in a list for each orcid id
    def create_user_download_object(self, dataset_details_object, download_stats):
        users = {}
        for dataset in dataset_details_object:

            # filter to only have datsets with downloads
            downloadInfo = [d for d in download_stats if dataset['id'] == d['datasetId']]

            downloadInfo = self.add_dataset_name_to_download_info(dataset, downloadInfo)
            for contributor in dataset['contributors']:
                orcid_id = contributor['orcid']

                # Add the download info with an orcid id as a key
                if orcid_id not in users.keys():
                    users[orcid_id] = {}
                    users[orcid_id]['datasets'] = downloadInfo
                else:
                    users[orcid_id]['datasets'] += downloadInfo

        return users

    def add_dataset_name_to_download_info(self, dataset, downloadInfo):
        for i in range(0, len(downloadInfo)):
            downloadInfo[i]['name'] = dataset['name']
        return downloadInfo


    # send email using sendgrid
    def send_email(self, email_address, email_body):
        if self.debug_mode:
            email_destination = self.debug_email
            return self.send_grid.sendgrid_email_with_unsubscribe_group(Config.METRICS_EMAIL_ADDRESS,
                                                                    email_destination,
                                                                    'SPARC monthly dataset download summary',
                                                                    email_body)
        elif Config.DEPLOY_ENV is 'production':
            email_destination = email_address
            return self.send_grid.sendgrid_email_with_unsubscribe_group(Config.METRICS_EMAIL_ADDRESS,
                                                                    email_destination,
                                                                    'SPARC monthly dataset download summary',
                                                                    email_body)

