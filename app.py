from flask import Flask, Blueprint, jsonify, request
import boto3
from .config import Config
from .email_sender import EmailSender
import json 
import requests
from flask_marshmallow import Marshmallow
from blackfynn import Blackfynn
from .serializer import ContactRequestSchema
# from pymongo import MongoClient
import logging


app = Flask(__name__)
ma = Marshmallow(app)
email_sender = EmailSender()
mongo = None
bf = None
s3 = boto3.client('s3',
                  aws_access_key_id=Config.SPARC_PORTAL_AWS_KEY,
                  aws_secret_access_key=Config.SPARC_PORTAL_AWS_SECRET,
                  region_name='us-east-1'
                  )


@app.before_first_request
def connect_to_blackfynn():
    global bf
    bf = Blackfynn(
        api_token=Config.BLACKFYNN_API_TOKEN,
        api_secret=Config.BLACKFYNN_API_SECRET,
        env_override=False,
        host=Config.BLACKFYNN_API_HOST
    )

# @app.before_first_request
# def connect_to_mongodb():
#     global mongo
#     mongo = MongoClient(Config.MONGODB_URI)


api_blueprint = Blueprint('api', __name__, url_prefix='/api')


@api_blueprint.route('/health')
def health():
    return json.dumps({ "status": "healthy" })


@api_blueprint.route("/contact", methods=["POST"])
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
@api_blueprint.route('/download')
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


tags = 'tags=simcore'


@api_blueprint.route('/sim/dataset')
def sim_datasets():
    if request.method == 'GET':
        req = requests.get('{}/datasets?{}'.format(Config.DISCOVER_API_HOST, tags))
        json = req.json()
        return jsonify(json)


@api_blueprint.route('/sim/search-dataset')
def sim_search_datasets():
    if request.method == 'GET':
        query = request.args.get('query')
        req = requests.get('{}/search/datasets?query={}'.format(Config.DISCOVER_API_HOST, query))
        json = req.json()
        # Filter only datasets with tag 'simcore'
        json['datasets'] = list(filter(lambda dataset: ('simcore' in dataset.get('tags', [])), json.get('datasets', [])))
        return jsonify(json)


@api_blueprint.route('/sim/dataset/<id>')
def sim_dataset(id):
    if request.method == 'GET':
        req = requests.get('{}/datasets/{}'.format(Config.DISCOVER_API_HOST, id))
        json = req.json()
        inject_markdown(json)
        inject_template_data(json)
        return jsonify(json)


def inject_markdown(resp):
    if 'readme' in resp:
        mark_req = requests.get(resp.get('readme'))
        resp['markdown'] = mark_req.text


def inject_template_data(resp):
    from botocore.exceptions import ClientError
    import json

    id = resp.get('id')
    version = resp.get('version')
    if (id is None or version is None):
        return

    try:
        response = s3.get_object(Bucket='blackfynn-discover-use1',
                                 Key='{}/{}/files/template.json'.format(id, version),
                                 RequestPayer='requester')
    except ClientError as e:
        # If the file is not under folder 'files', check under folder 'packages'
        logging.warning('Required file template.json was not found under /files folder, trying under /packages...')
        try:
            response = s3.get_object(Bucket='blackfynn-discover-use1',
                                     Key='{}/{}/packages/template.json'.format(id, version),
                                     RequestPayer='requester')
        except ClientError as e2:
            logging.error(e2)
            return

    template = response['Body'].read()

    try:
        template_json = json.loads(template)
    except ValueError as e:
        logging.error(e)
        return

    resp['study'] = {'uuid': template_json.get('uuid'),
                     'name': template_json.get('name'),
                     'description': template_json.get('description')}


app.register_blueprint(api_blueprint)