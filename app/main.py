from flask import Flask, request
from flask_cors import CORS
import boto3
from app.config import Config
from scripts.email_sender import EmailSender
import json 
# import requests
from flask_marshmallow import Marshmallow
# from blackfynn import Blackfynn
from app.serializer import ContactRequestSchema
# from pymongo import MongoClient
# import logging


app = Flask(__name__)
# set environment variable
app.config['ENV'] = Config.DEPLOY_ENV

cors = CORS(app, resources={r"*": {"origins": Config.SPARC_APP_HOST}})

ma = Marshmallow(app)
email_sender = EmailSender()
mongo = None
bf = None
s3 = boto3.client('s3',
                  aws_access_key_id=Config.SPARC_PORTAL_AWS_KEY,
                  aws_secret_access_key=Config.SPARC_PORTAL_AWS_SECRET,
                  region_name='us-east-1'
                  )


# @app.before_first_request
# def connect_to_blackfynn():
#     global bf
#     bf = Blackfynn(
#         api_token=Config.BLACKFYNN_API_TOKEN,
#         api_secret=Config.BLACKFYNN_API_SECRET,
#         env_override=False,
#         host=Config.BLACKFYNN_API_HOST
#     )

# @app.before_first_request
# def connect_to_mongodb():
#     global mongo
#     mongo = MongoClient(Config.MONGODB_URI)


@app.route('/health')
def health():
    return json.dumps({ "status": "healthy" })


@app.route("/contact", methods=["POST"])
def contact():
    data = json.loads(request.data)
    contact_request = ContactRequestSchema().load(data)

    name = contact_request["name"]
    email = contact_request["email"]
    message = contact_request["message"]

    email_sender.send_email(name, email, message)

    return json.dumps({"status": "sent"})

# Returns a list of embargoed (unpublished) datasets
# @api_blueprint.route('/datasets/embargo')
# def embargo():
#     collection = mongo[Config.MONGODB_NAME][Config.MONGODB_COLLECTION]
#     embargo_list = list(collection.find({}, {'_id':0}))
#     return json.dumps(embargo_list)


# Download a file from S3
@app.route('/download')
def create_presigned_url(expiration=3600):
    bucket_name = 'blackfynn-discover-use1'
    key = request.args.get('key')
    response = s3.generate_presigned_url('get_object',
                                         Params={
                                             'Bucket': bucket_name,
                                             'Key': key,
                                             'RequestPayer': 'requester'
                                         },
                                         ExpiresIn=expiration)

    return response
