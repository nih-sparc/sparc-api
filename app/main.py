import atexit
import base64
import boto3
import json
import logging
import requests

from apscheduler.schedulers.background import BackgroundScheduler
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
from flask import Flask, abort, jsonify, request, redirect
from flask_cors import CORS
from flask_marshmallow import Marshmallow
from pennsieve import Pennsieve
from pennsieve.base import UnauthorizedException as PSUnauthorizedException
from PIL import Image
from requests.auth import HTTPBasicAuth

from app.scicrunch_requests import create_doi_query, create_filter_request, create_facet_query, create_doi_aggregate, create_title_query, \
    create_identifier_query, create_pennsieve_identifier_query, create_field_query, create_request_body_for_curies, create_onto_term_query, \
    create_multiple_doi_query, create_multiple_discoverId_query
from scripts.email_sender import EmailSender, feedback_email, issue_reporting_email
from threading import Lock
from xml.etree import ElementTree

from app.config import Config
from app.dbtable import MapTable, ScaffoldTable
from app.scicrunch_process_results import reform_dataset_results, process_results, reform_aggregation_results, reform_curies_results
from app.serializer import ContactRequestSchema
from app.utilities import img_to_base64_str
from app.osparc import run_simulation
from app.biolucida_process_results import process_results as process_biolucida_results

app = Flask(__name__)
# set environment variable
app.config["ENV"] = Config.DEPLOY_ENV

CORS(app)

ma = Marshmallow(app)
email_sender = EmailSender()

ps = None
s3 = boto3.client(
    "s3",
    aws_access_key_id=Config.SPARC_PORTAL_AWS_KEY,
    aws_secret_access_key=Config.SPARC_PORTAL_AWS_SECRET,
    region_name="us-east-1",
)

biolucida_lock = Lock()

osparc_data = {}

try:
    maptable = MapTable(Config.DATABASE_URL)
except AttributeError:
    maptable = None

try:
    scaffoldtable = ScaffoldTable(Config.DATABASE_URL)
except AttributeError:
    scaffoldtable = None


class Biolucida(object):
    _token = ''
    _expiry_date = datetime.now() + timedelta(999999)
    _pending_authentication = False

    @staticmethod
    def set_token(value):
        Biolucida._token = value

    def token(self):
        return self._token

    @staticmethod
    def set_expiry_date(value):
        Biolucida._expiry_date = value

    def expiry_date(self):
        return self._expiry_date

    @staticmethod
    def set_pending_authentication(value):
        Biolucida._pending_authentication = value

    def pending_authentication(self):
        return self._pending_authentication


@app.errorhandler(404)
def resource_not_found(e):
    return jsonify(error=str(e)), 404


@app.before_first_request
def connect_to_pennsieve():
    global ps
    try:
        ps = Pennsieve(
            api_token=Config.PENNSIEVE_API_TOKEN,
            api_secret=Config.PENNSIEVE_API_SECRET,
            env_override=False,
            host=Config.PENNSIEVE_API_HOST
        )
    except requests.exceptions.HTTPError as err:
        logging.error("Unable to connect to Pennsieve host")
        logging.error(err)
    except PSUnauthorizedException as err:
        logging.error("Unable to authorise with Pennsieve Api")
        logging.error(err)
    except Exception as err:
        logging.error("Unknown Error")
        logging.error(err)


viewers_scheduler = BackgroundScheduler()


@app.before_first_request
def get_osparc_file_viewers():
    logging.info('Getting oSPARC viewers')
    # Gets a list of default viewers.
    req = requests.get(url=f'{Config.OSPARC_API_HOST}/viewers/default')
    viewers = req.json()
    table = build_filetypes_table(viewers["data"])
    osparc_data["file_viewers"] = table
    if not viewers_scheduler.running:
        logging.info('Starting scheduler for oSPARC viewers acquisition')
        viewers_scheduler.start()


# Gets oSPARC viewers before the first request after startup and then once a day.
viewers_scheduler.add_job(func=get_osparc_file_viewers, trigger="interval", days=1)


def shutdown_scheduler():
    logging.info('Stopping scheduler for oSPARC viewers acquisition')
    if viewers_scheduler.running:
        viewers_scheduler.shutdown()


atexit.register(shutdown_scheduler)


@app.route("/health")
def health():
    return json.dumps({"status": "healthy"})


@app.route("/contact", methods=["POST"])
def contact():
    data = json.loads(request.data)
    contact_request = ContactRequestSchema().load(data)

    name = contact_request["name"]
    email = contact_request["email"]
    message = contact_request["message"]

    email_sender.send_email(name, email, message)
    email_sender.sendgrid_email(Config.SES_SENDER, email, 'Feedback submission', feedback_email.substitute({ 'message': message }))

    return json.dumps({"status": "sent"})


def create_s3_presigned_url(key, content_type, expiration):
    response = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": Config.S3_BUCKET_NAME, "Key": key, "RequestPayer": "requester", "ResponseContentType": content_type},
        ExpiresIn=expiration,
    )

    return response


@app.route("/download-link")
def create_presigned_url(expiration=3600):
    key = request.args.get("key")
    content_type = request.args.get("contentType") or "application/octet-stream"

    return create_s3_presigned_url(key, content_type, expiration)


# Download a file from S3
@app.route("/download")
def download_file(expiration=3600):
    key = request.args.get("key")
    content_type = request.args.get("contentType") or "application/octet-stream"

    presigned_url = create_s3_presigned_url(key, content_type, expiration)
    return redirect(presigned_url)


@app.route("/thumbnail/neurolucida")
def thumbnail_from_neurolucida_file():
    query_args = request.args
    if 'version' not in query_args or 'datasetId' not in query_args or 'path' not in query_args:
        return abort(400, description=f"Query arguments are not valid.")

    url = f"{Config.NEUROLUCIDA_HOST}/thumbnail"
    response = requests.get(url, params=query_args)
    if response.status_code == 200:
        if response.headers.get('Content-Type', 'unknown') == 'image/png':
            return base64.b64encode(response.content)

    abort(400, 'Failed to retrieve thumbnail.')


@app.route("/thumbnail/segmentation")
def extract_thumbnail_from_xml_file():
    """
    Extract a thumbnail from a mbf xml file.
    First phase is to find the thumbnail element in the xml document.
    Second phase is to convert the xml to a base64 png.
    """
    query_args = request.args
    if 'path' not in query_args:
        return abort(400, description=f"Query arguments are not valid.")

    path = query_args['path']
    resource = None
    start_tag_found = False
    end_tag_found = False
    start_byte = 0
    offset = 256000
    end_byte = offset
    while not start_tag_found or not end_tag_found:
        try:
            response = s3.get_object(
                Bucket=Config.S3_BUCKET_NAME,
                Key=path,
                Range=f"bytes={start_byte}-{end_byte}",
                RequestPayer="requester"
            )
        except ClientError as ex:
            if ex.response['Error']['Code'] == 'NoSuchKey':
                return abort(404, description=f"Could not find file: '{path}'")
            else:
                return abort(404, description=f"Unknown error for file: '{path}'")

        resource = response["Body"].read().decode('UTF-8')
        start_tag_found = '<thumbnail ' in resource
        end_tag_found = '</thumbnail>' in resource
        if start_tag_found and not end_tag_found:
            end_byte += offset
        else:
            start_byte += offset
            end_byte += offset

        if len(resource) < offset:
            return abort(404, description=f"Could not find thumbnail in file: '{path}'")

    if resource is None:
        return abort(404, description=f"Could not find thumbnail in file: '{path}'")

    start_thumbnail_element = resource[resource.find('<thumbnail '):]
    thumbnail_xml = start_thumbnail_element[:start_thumbnail_element.find('</thumbnail>')] + '</thumbnail>'
    xml = ElementTree.fromstring(thumbnail_xml)
    size_info = xml.attrib
    im_data = ''
    for child in xml:
        im_data += child.text[2:]

    byte_im_data = bytes.fromhex(im_data)
    im = Image.frombytes("RGB", (int(size_info['rows']), int(size_info['cols'])), byte_im_data)
    base64_form = img_to_base64_str(im)

    return base64_form


@app.route("/exists/<path:path>")
def url_exists(path):
    try:
        head_response = s3.head_object(
            Bucket=Config.S3_BUCKET_NAME,
            Key=path,
            RequestPayer="requester"
        )
    except ClientError:
        return {'exists': 'false'}

    content_length = head_response.get('ContentLength', 0)

    if content_length > 0:
        return {'exists': 'true'}

    return {'exists': 'false'}


def fetch_discover_file_information(uri):
    # Fudge the URI from Sci-crunch
    uri = uri.replace('https://api.pennsieve.io/', 'https://api.pennsieve.io/discover/')
    uri = uri[:uri.rfind('/')]

    r = requests.get(uri)
    return r.json()


@app.route("/s3-resource/discover_path")
def get_discover_path():
    uri = request.args.get('uri')

    json_response = fetch_discover_file_information(uri)
    if 'totalCount' in json_response and json_response['totalCount'] == 1:
        file_info = json_response['files'][0]
        return file_info['path']

    return abort(404, description=f'Failed to retrieve uri {uri}')


@app.route("/s3-resource/presign_discover_file_uri")
def presign_resource_url():
    uri = request.args.get('uri')

    json_response = fetch_discover_file_information(uri)
    if 'totalCount' in json_response and json_response['totalCount'] == 1:
        file_info = json_response['files'][0]
        key = file_info['uri'].replace('s3://' + Config.S3_BUCKET_NAME + '/', '')
        content_type = request.args.get("contentType") or "application/octet-stream"

        return create_s3_presigned_url(key, content_type, 3600)

    return abort(404, description=f'Failed to retrieve uri {uri}')


# Reverse proxy for objects from S3, a simple get object
# operation. This is used by scaffoldvuer and its
# important to keep the relative <path> for accessing
# other required files.
@app.route("/s3-resource/<path:path>")
def direct_download_url(path):
    head_response = s3.head_object(
        Bucket=Config.S3_BUCKET_NAME,
        Key=path,
        RequestPayer="requester"
    )

    content_length = head_response.get('ContentLength', Config.DIRECT_DOWNLOAD_LIMIT)
    if content_length and content_length > Config.DIRECT_DOWNLOAD_LIMIT:  # 20 MB
        return abort(413, description=f"File too big to download: {content_length}")

    response = s3.get_object(
        Bucket=Config.S3_BUCKET_NAME,
        Key=path,
        RequestPayer="requester"
    )

    encode_base64 = request.args.get("encodeBase64")
    resource = response["Body"].read()
    if encode_base64 is not None:
        return base64.b64encode(resource)

    return resource


@app.route("/scicrunch-dataset/<doi1>/<doi2>")
def sci_doi(doi1, doi2):
    doi = doi1.replace('DOI:', '') + '/' + doi2
    data = create_doi_query(doi)

    try:
        response = requests.post(
            f'{Config.SCI_CRUNCH_HOST}/_search?api_key={Config.KNOWLEDGEBASE_KEY}',
            json=data)
        return response.json()
    except requests.exceptions.HTTPError as err:
        logging.error(err)
        return json.dumps({'error': err})


# /pubmed/<id> Used as a proxy for making requests to pubmed
@app.route("/pubmed/<id_>")
@app.route("/pubmed/<id_>/")
def pubmed(id_):
    try:
        response = requests.get(f'https://pubmed.ncbi.nlm.nih.gov/{id_}/')
        return response.text
    except requests.exceptions.HTTPError as err:
        logging.error(err)
        return json.dumps({'error': err})


# /scicrunch-query-string/: Returns results for given organ curie. These can be processed by the sidebar
@app.route("/scicrunch-query-string/")
def sci_organ():
    fields = request.args.getlist('field')
    curie = request.args.get('curie')
    size = request.args.get('size')
    from_ = request.args.get('from')

    data = create_field_query(fields, curie, size, from_)

    try:
        response = requests.post(
            f'{Config.SCI_CRUNCH_HOST}/_search?api_key={Config.KNOWLEDGEBASE_KEY}',
            json=data)
        return process_results(response.json())
    except requests.exceptions.HTTPError as err:
        logging.error(err)
        return json.dumps({'error': err})


@app.route("/dataset_info/using_doi")
def get_dataset_info_doi():
    doi = request.args.get('doi')
    raw = request.args.get('raw_response')
    query = create_doi_query(doi)

    if raw is None:
        return reform_dataset_results(dataset_search(query))

    return dataset_search(query)

@app.route("/dataset_info/using_multiple_dois")
@app.route("/dataset_info/using_multiple_dois/")
def get_dataset_info_dois():
    dois = request.args.getlist('dois')
    query = create_multiple_doi_query(dois)

    return process_results(dataset_search(query))

@app.route("/dataset_info/using_multiple_discoverIds")
@app.route("/dataset_info/using_multiple_discoverIds/")
def get_dataset_info_discoverIds():
    discoverIds = request.args.getlist('discoverIds')
    query = create_multiple_discoverId_query(discoverIds)

    return process_results(dataset_search(query))


@app.route("/dataset_info/using_title")
def get_dataset_info_title():
    title = request.args.get('title')
    query = create_title_query(title)

    return reform_dataset_results(dataset_search(query))


@app.route("/dataset_info/using_object_identifier")
def get_dataset_info_object_identifier():
    identifier = request.args.get('identifier')
    query = create_identifier_query(identifier)

    return reform_dataset_results(dataset_search(query))


@app.route("/dataset_info/using_pennsieve_identifier")
def get_dataset_info_pennsieve_identifier():
    identifier = request.args.get('identifier')
    query = create_pennsieve_identifier_query(identifier)

    return reform_dataset_results(dataset_search(query))


@app.route("/segmentation_info/")
def get_segmentation_info_from_file():
    dataset_path = request.args.get('dataset_path')
    try:
        response = s3.get_object(
            Bucket=Config.S3_BUCKET_NAME,
            Key=dataset_path,
            RequestPayer="requester"
        )
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            return abort(404, description=f"Could not find file: '{dataset_path}'")
        else:
            return abort(404, description=f"Unknown error for file: '{dataset_path}'")

    resource = response["Body"].read().decode('UTF-8')
    xml = ElementTree.fromstring(resource)
    subject_element = xml.find('./sparcdata/subject')
    info = {}
    if subject_element is not None:
        info['subject'] = subject_element.attrib
    else:
        info['subject'] = {'age': '', 'sex': '', 'species': '', 'subjectid': ''}

    atlas_element = xml.find('./sparcdata/atlas')
    if atlas_element is not None:
        info['atlas'] = atlas_element.attrib
    else:
        info['atlas'] = {'organ': ''}

    return info


@app.route("/current_doi_list")
def get_all_doi():
    query = create_doi_aggregate()
    results = reform_aggregation_results(dataset_search(query))
    doi_results = []
    for result in results['doi']['buckets']:
        doi_results.append(result['key']['curie'])

    return {'results': doi_results}


def dataset_search(query):
    try:
        payload = query

        params = {
            "api_key": Config.KNOWLEDGEBASE_KEY
        }
        response = requests.post(f'{Config.SCI_CRUNCH_HOST}/_search',
                                 json=payload, params=params)

        return response.json()
    except requests.exceptions.HTTPError as err:
        logging.error(err)

        return jsonify({'error': err})


# /search/: Returns sci-crunch results for a given <search> query
@app.route("/search/", defaults={'query': '', 'limit': 10, 'start': 0})
@app.route("/search/<query>")
def kb_search(query, limit=10, start=0):
    try:
        if request.args.get('limit') is not None:
            limit = request.args.get('limit')
        if request.args.get('query') is not None:
            query = request.args.get('query')
        if request.args.get('start') is not None:
            start = request.args.get('start')

        # print(f'{Config.SCI_CRUNCH_HOST}/_search?q={query}&size={limit}&from={start}&api_key={Config.KNOWLEDGEBASE_KEY}')
        response = requests.get(f'{Config.SCI_CRUNCH_HOST}/_search?q={query}&size={limit}&from={start}&api_key={Config.KNOWLEDGEBASE_KEY}')
        return process_results(response.json())
    except requests.exceptions.HTTPError as err:
        logging.error(err)
        return json.dumps({'error': err})


# /filter-search/: Returns sci-crunch results with optional params for facet filtering, sizing, and pagination
@app.route("/filter-search/", defaults={'query': ''})
@app.route("/filter-search/<query>/")
def filter_search(query):
    terms = request.args.getlist('term')
    facets = request.args.getlist('facet')
    size = request.args.get('size')
    start = request.args.get('start')

    # Create request
    data = create_filter_request(query, terms, facets, size, start)

    # Send request to sci-crunch
    try:
        response = requests.post(
            f'{Config.SCI_CRUNCH_HOST}/_search?api_key={Config.KNOWLEDGEBASE_KEY}',
            json=data)
        results = process_results(response.json())
    except requests.exceptions.HTTPError as err:
        logging.error(err)
        return jsonify({'error': str(err), 'message': 'SciCrunch is not currently reachable, please try again later'}), 502
    except json.JSONDecodeError:
        return jsonify({'message': 'Could not parse SciCrunch output, please try again later',
                        'error': 'JSONDecodeError'}), 502
    return results


# /get-facets/: Returns available sci-crunch facets for filtering over given a <type> ('species', 'gender' etc)
@app.route("/get-facets/<type_>")
def get_facets(type_):
    # Create facet query
    type_map, data = create_facet_query(type_)

    # Make a request for each sci-crunch parameter
    results = []
    for path in type_map[type_]:
        data['aggregations'][f'{type_}']['terms']['field'] = path
        response = requests.post(
            f'{Config.SCI_CRUNCH_HOST}/_search?api_key={Config.KNOWLEDGEBASE_KEY}',
            json=data)
        try:
            json_result = response.json()
            results.append(json_result)
        except json.JSONDecodeError:
            return jsonify({'message': 'Could not parse SciCrunch output, please try again later',
                            'error': 'JSONDecodeError'}), 502

    # Select terms from the results
    terms = []
    for result in results:
        terms += result['aggregations'][f'{type_}']['buckets']

    return jsonify(terms)


def inject_markdown(resp):
    if "readme" in resp:
        mark_req = requests.get(resp.get("readme"))
        resp["markdown"] = mark_req.text


def inject_template_data(resp):
    id_ = resp.get("id")
    version = resp.get("version")
    if id_ is None or version is None:
        return

    try:
        response = s3.get_object(
            Bucket="pennsieve-prod-discover-publish-use1",
            Key="{}/{}/files/template.json".format(id_, version),
            RequestPayer="requester",
        )
    except ClientError:
        # If the file is not under folder 'files', check under folder 'packages'
        logging.warning(
            "Required file template.json was not found under /files folder, trying under /packages..."
        )
        try:
            response = s3.get_object(
                Bucket="pennsieve-prod-discover-publish-use1",
                Key="{}/{}/packages/template.json".format(id_, version),
                RequestPayer="requester",
            )
        except ClientError as e:
            logging.error(e)
            return

    template = response["Body"].read()

    try:
        template_json = json.loads(template)
    except ValueError as e:
        logging.error(e)
        return

    resp["study"] = {
        "uuid": template_json.get("uuid"),
        "name": template_json.get("name"),
        "description": template_json.get("description"),
    }


# Constructs a table with where keys are the normalized (lowercased) file types
# and the values an array of possible viewers
def build_filetypes_table(osparc_viewers):
    table = {}
    for viewer in osparc_viewers:
        filetype = viewer["file_type"].lower()
        del viewer["file_type"]
        if not table.get(filetype, False):
            table[filetype] = []
        table[filetype].append(viewer)
    return table


@app.route("/sim/dataset/<id_>")
def sim_dataset(id_):
    if request.method == "GET":
        req = requests.get("{}/datasets/{}".format(Config.DISCOVER_API_HOST, id_))
        if req.ok:
            json_data = req.json()
            inject_markdown(json_data)
            inject_template_data(json_data)
            return jsonify(json_data)
        abort(404, description="Resource not found")


@app.route("/get_osparc_data")
def get_osparc_data():
    return jsonify(osparc_data)


@app.route("/project/<project_id>", methods=["GET"])
def datasets_by_project_id(project_id):
    # 1 - call discover to get awards on all datasets (let put a very high limit to make sure we do not miss any)

    req = requests.get(
        "{}/search/records?limit=1000&offset=0&model=award".format(
            Config.DISCOVER_API_HOST
        )
    )

    records = req.json()["records"]

    # 2 - filter response to retain only awards with project_id
    result = filter(lambda x: "award_id" in x["properties"] and x["properties"]["award_id"] == project_id, records)

    ids = map(lambda x: str(x["datasetId"]), result)

    separator = "&ids="

    list_ids = separator.join(ids)

    # 3 - get the datasets from the list of ids from #2

    if len(list_ids) > 0:
        return requests.get(
            "{}/datasets?ids={}".format(Config.DISCOVER_API_HOST, list_ids)
        ).json()
    else:
        abort(404, description="Resource not found")


@app.route("/get_owner_email/<int:owner_id>", methods=["GET"])
def get_owner_email(owner_id):
    # Filter to find user based on provided int id
    org = ps._api._organization
    members = ps._api.organizations.get_members(org)
    res = [x for x in members if x.int_id == owner_id]

    if not res:
        abort(404, description="Owner not found")
    else:
        return jsonify({"email": res[0].email})


@app.route("/thumbnail/<image_id>", methods=["GET"])
def thumbnail_by_image_id(image_id, recursive_call=False):
    bl = Biolucida()

    with biolucida_lock:
        if not bl.token():
            authenticate_biolucida()

    url = Config.BIOLUCIDA_ENDPOINT + "/thumbnail/{0}".format(image_id)
    headers = {
        'token': bl.token(),
    }

    response = requests.request("GET", url, headers=headers)
    encoded_content = base64.b64encode(response.content)
    # Response from this endpoint is binary on success so the easiest thing to do is
    # check for an error response in encoded form.
    if encoded_content == b'eyJzdGF0dXMiOiJBZG1pbiB1c2VyIGF1dGhlbnRpY2F0aW9uIHJlcXVpcmVkIHRvIHZpZXcvZWRpdCB1c2VyIGluZm8uIFlvdSBtYXkgbmVlZCB0byBsb2cgb3V0IGFuZCBsb2cgYmFjayBpbiB0byByZXZlcmlmeSB5b3VyIGNyZWRlbnRpYWxzLiJ9' \
            and not recursive_call:
        # Authentication failure, try again after resetting token.
        with biolucida_lock:
            bl.set_token('')

        encoded_content = thumbnail_by_image_id(image_id, True)

    return encoded_content


@app.route("/image/<image_id>", methods=["GET"])
def image_info_by_image_id(image_id):
    url = Config.BIOLUCIDA_ENDPOINT + "/image/{0}".format(image_id)
    response = requests.request("GET", url)
    return response.json()


@app.route("/image_search/<dataset_id>", methods=["GET"])
def image_search_by_dataset_id(dataset_id):
    url = Config.BIOLUCIDA_ENDPOINT + "/imagemap/search_dataset/discover/{0}".format(dataset_id)
    response = requests.request("GET", url)

    return response.json()


@app.route("/image_xmp_info/<image_id>", methods=["GET"])
def image_xmp_info(image_id):
    url = Config.BIOLUCIDA_ENDPOINT + "/image/xmpmetadata/{0}".format(image_id)
    result = requests.request("GET", url)

    response = result.json()
    if response['status'] == 'success':
        return process_biolucida_results(response['data'])

    return abort(400, description=f"XMP info not found for {image_id}")


def authenticate_biolucida():
    bl = Biolucida()
    url = Config.BIOLUCIDA_ENDPOINT + "/authenticate"

    payload = {'username': Config.BIOLUCIDA_USERNAME,
               'password': Config.BIOLUCIDA_PASSWORD,
               'token': ''}
    files = [
    ]
    headers = {}

    response = requests.request("POST", url, headers=headers, data=payload, files=files)
    if response.status_code == requests.codes.ok:
        content = response.json()
        bl.set_token(content['token'])


def get_share_link(table):
    # Do not commit to database when testing
    commit = True
    if app.config["TESTING"]:
        commit = False
    if table:
        json_data = request.get_json()
        if json_data and 'state' in json_data:
            state = json_data['state']
            uuid = table.pushState(state, commit)
            return jsonify({"uuid": uuid})
        abort(400, description="State not specified")
    else:
        abort(404, description="Database not available")


def get_saved_state(table):
    if table:
        json_data = request.get_json()
        if json_data and 'uuid' in json_data:
            uuid = json_data['uuid']
            state = table.pullState(uuid)
            if state:
                return jsonify({"state": table.pullState(uuid)})
        abort(400, description="Key missing or did not find a match")
    else:
        abort(404, description="Database not available")


# Get the share link for the current map content.
@app.route("/map/getshareid", methods=["POST"])
def get_map_share_link():
    return get_share_link(maptable)


# Get the map state using the share link id.
@app.route("/map/getstate", methods=["POST"])
def get_map_state():
    return get_saved_state(maptable)


# Get the share link for the current map content.
@app.route("/scaffold/getshareid", methods=["POST"])
def get_scaffold_share_link():
    return get_share_link(scaffoldtable)


# Get the map state using the share link id.
@app.route("/scaffold/getstate", methods=["POST"])
def get_scaffold_state():
    return get_saved_state(scaffoldtable)


@app.route("/tasks", methods=["POST"])
def create_wrike_task():
    json_data = request.get_json()
    if json_data and 'title' in json_data and 'description' in json_data:
        title = json_data["title"]
        description = json_data["description"]
        hed = {'Authorization': 'Bearer ' + Config.WRIKE_TOKEN}
        url = 'https://www.wrike.com/api/v4/folders/IEADBYQEI4MM37FH/tasks'

        data = {
            "title": title,
            "description": description,
            "customStatus": "IEADBYQEJMBJODZU",
            "followers": [Config.CCB_HEAD_WRIKE_ID, Config.DAT_CORE_TECH_LEAD_WRIKE_ID, Config.MAP_CORE_TECH_LEAD_WRIKE_ID, Config.K_CORE_TECH_LEAD_WRIKE_ID,
                          Config.SIM_CORE_TECH_LEAD_WRIKE_ID, Config.MODERATOR_WRIKE_ID],
            "responsibles": [Config.CCB_HEAD_WRIKE_ID, Config.DAT_CORE_TECH_LEAD_WRIKE_ID, Config.MAP_CORE_TECH_LEAD_WRIKE_ID, Config.K_CORE_TECH_LEAD_WRIKE_ID,
                             Config.SIM_CORE_TECH_LEAD_WRIKE_ID, Config.MODERATOR_WRIKE_ID],
            "follow": False,
            "dates": {"type": "Backlog"}
        }

        resp = requests.post(
            url=url,
            json=data,
            headers=hed
        )

        if resp.status_code == 200:

            if json_data['userEmail']:
                email_sender.sendgrid_email(Config.SES_SENDER, json_data['userEmail'], 'Issue reporting', issue_reporting_email.substitute({ 'message': json_data['description'] }))

            return jsonify(
                title=title,
                description=description,
                task_id=resp.json()["data"][0]["id"]
            )
        else:
            return resp.json()
    else:
        abort(400, description="Missing title or description")


@app.route("/mailchimp", methods=["POST"])
def subscribe_to_mailchimp():
    json_data = request.get_json()
    if json_data and 'email_address' in json_data and 'first_name' in json_data and 'last_name' in json_data:
        email_address = json_data["email_address"]
        first_name = json_data['first_name']
        last_name = json_data['last_name']
        auth = HTTPBasicAuth('AnyUser', Config.MAILCHIMP_API_KEY)
        url = 'https://us2.api.mailchimp.com/3.0/lists/c81a347bd8/members'

        data = {
            "email_address": email_address,
            "status": "subscribed",
            "merge_fields": {
                "FNAME": first_name,
                "LNAME": last_name
            }
        }
        resp = requests.post(
            url=url,
            json=data,
            auth=auth
        )

        if resp.status_code == 200:
            return jsonify(
                email_address=email_address,
                id=resp.json()["id"]
            )
        else:
            return resp.json()
    else:
        abort(400, description="Missing email_address, first_name or last_name")


# Get list of available name / curie pair
@app.route("/get-organ-curies/", defaults={'query': ''})
@app.route("/get-organ-curies/<query>/")
def get_available_uberonids(query):
    species = request.args.getlist('species')

    requestBody = create_request_body_for_curies(species)

    result = {}

    response = requests.post(
        f'{Config.SCI_CRUNCH_HOST}/_search?api_key={Config.KNOWLEDGEBASE_KEY}',
        json=requestBody)
    try:
        result = reform_curies_results(response.json())
    except BaseException:
        return jsonify({'message': 'Could not parse SciCrunch output, please try again later',
                        'error': 'BaseException'}), 502

    return jsonify(result)


@app.route("/simulation_ui_file/<identifier>")
def simulation_ui_file(identifier):
    results = process_results(dataset_search(create_pennsieve_identifier_query(identifier)))
    results_json = json.loads(results.data)

    try:
        item = results_json["results"][0]
        uri = item["s3uri"]
        path = item["abi-simulation-file"][0]["dataset"]["path"]
        key = f"{uri}files/{path}".replace(f"s3://{Config.S3_BUCKET_NAME}/", "")

        return jsonify(json.loads(direct_download_url(key)))
    except Exception:
        abort(404, description="no simulation UI file could be found")


@app.route("/simulation", methods=["POST"])
def simulation():
    data = request.get_json()

    if data and "model_url" in data and "json_config" in data:
        return json.dumps(run_simulation(data["model_url"], data["json_config"]))
    else:
        abort(400, description="Missing model URL and/or JSON configuration")


@app.route("/pmr_latest_exposure", methods=["POST"])
def pmr_latest_exposure():
    data = request.get_json()

    if data and "workspace_url" in data:
        try:
            resp = requests.get(data["workspace_url"],
                                headers={"Accept": "application/vnd.physiome.pmr2.json.1"})
            if resp.status_code == 200:
                try:
                    # Return the latest exposure for the given workspace.
                    url = resp.json()["collection"]["items"][0]["links"][0]["href"]
                except:
                    # There is no latest exposure for the given workspace.
                    url = ""
                return jsonify(
                    url=url
                )
            else:
                return resp.json()
        except:
            abort(400, description="Invalid workspace URL")
    else:
        abort(400, description="Missing workspace URL")


@app.route("/onto_term_lookup")
def find_by_onto_term():
    term = request.args.get('term')

    headers = {
        'Accept': 'application/json',
    }

    params = {
        "api_key": Config.KNOWLEDGEBASE_KEY
    }

    query = create_onto_term_query(term)

    response = requests.get(f'{Config.SCI_CRUNCH_INTERLEX_HOST}/_search', headers=headers, params=params, json=query)

    results = response.json()
    hits = results['hits']['hits']
    total = results['hits']['total']
    if total == 1:
        result = hits[0]
        json_data = result['_source']
    else:
        json_data = {'label': 'not found'}

    return json_data
