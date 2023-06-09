import logging
import boto3
from app.config import Config
import requests
import json



# Returns pennsieve api token valid for 24 hours

def pennsieve_login():
    r = requests.get(f"{Config.PENNSIEVE_API_HOST}/authentication/cognito-config")
    r.raise_for_status()

    cognito_app_client_id = r.json()["tokenPool"]["appClientId"]
    cognito_region = r.json()["region"]

    cognito_idp_client = boto3.client(
        "cognito-idp",
        region_name=cognito_region,
        aws_access_key_id=Config.SPARC_PORTAL_AWS_KEY,
        aws_secret_access_key=Config.SPARC_PORTAL_AWS_SECRET,
    )

    login_response = cognito_idp_client.initiate_auth(
        AuthFlow="USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": Config.PENNSIEVE_API_TOKEN, "PASSWORD": Config.PENNSIEVE_API_SECRET},
        ClientId=cognito_app_client_id,
    )

    api_key = login_response["AuthenticationResult"]["AccessToken"]
    return api_key



def get_banner(pennsieve_temp_api_key, dataset_id):
    print(f"{Config.PENNSIEVE_API_HOST}/datasets/N%3Adataset%3A{dataset_id}/banner?api_key={pennsieve_temp_api_key}")
    r = requests.get(f"{Config.PENNSIEVE_API_HOST}/datasets/N%3Adataset%3A{dataset_id}/banner",
                     headers={"Authorization": f"Bearer {pennsieve_temp_api_key}"})
    r.raise_for_status()
    return r.json()
