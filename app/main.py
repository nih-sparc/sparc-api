import atexit

from app.metrics.pennsieve import get_download_count
from app.metrics.contentful import init_cf_cda_client, get_funded_projects_count, get_featured_datasets
from scripts.update_contentful_entries import update_all_events_sort_order, update_event_sort_order
from app.metrics.algolia import get_dataset_count, init_algolia_client, get_all_dataset_ids, get_all_dataset_uuids, get_associated_datasets
from app.metrics.ga import init_ga_reporting, get_ga_1year_sessions, init_gspread_client, append_contact, upload_file, init_drive_client
from scripts.update_featured_dataset_id import set_featured_dataset_id, get_featured_dataset_id_table_state
from scripts.update_protocol_metrics import update_protocol_metrics, get_protocol_metrics_table_state
from app.osparc.services import OSparcServices

import botocore
import markdown
import boto3
import hashlib
import hmac
import base64
import time
import hubspot
from hubspot.crm.contacts import ApiException
import json
import logging
import re
import requests
import uuid
from urllib.parse import urlparse

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.combining import OrTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from flask import Flask, abort, jsonify, request
from flask_cors import CORS
from flask_marshmallow import Marshmallow
from pennsieve import Pennsieve
from pennsieve2.direct import new_client
from pennsieve.base import UnauthorizedException as PSUnauthorizedException
from PIL import Image
from requests.auth import HTTPBasicAuth
from flask_caching import Cache

from app.scicrunch_requests import create_doi_query, create_filter_request, create_facet_query, create_doi_aggregate, create_title_query, \
    create_identifier_query, create_pennsieve_identifier_query, create_field_query, create_request_body_for_curies, create_onto_term_query, \
    create_multiple_doi_query, create_multiple_discoverId_query, create_anatomy_query, get_body_scaffold_dataset_id, \
    create_multiple_mimetype_query, create_citations_query
from scripts.email_sender import EmailSender, feedback_email, issue_reporting_email, creation_request_confirmation_email, anbc_form_creation_request_confirmation_email, service_form_submission_request_confirmation_email
from threading import Lock
from xml.etree import ElementTree

from app.config import Config
from app.dbtable import AnnotationTable, MapTable, ScaffoldTable, FeaturedDatasetIdSelectorTable, ProtocolMetricsTable
from app.scicrunch_process_results import process_results, process_get_first_scaffold_info, reform_aggregation_results, \
    reform_curies_results, reform_dataset_results, reform_related_terms, reform_anatomy_results
from app.serializer import ContactRequestSchema
from app.utilities import img_to_base64_str, get_path_from_mangled_list, get_extension
from app.osparc.osparc import start_simulation as do_start_simulation
from app.osparc.osparc import check_simulation as do_check_simulation
from app.biolucida_process_results import process_results as process_biolucida_results, process_result as process_biolucida_result

import uuid

logging.basicConfig()

app = Flask(__name__)

log_level = Config.LOG_LEVEL.upper()
app.logger.setLevel(getattr(logging, log_level, logging.WARNING))

executor = ThreadPoolExecutor(max_workers=8)

# set environment variable
app.config["ENV"] = Config.DEPLOY_ENV
cache = Cache(app, config={'CACHE_TYPE': 'SimpleCache', 'CACHE_DEFAULT_TIMEOUT': 300})

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

db_url = Config.DATABASE_URL
if db_url and db_url.startswith("postgres://"):
    db_url = db_url.replace("postgres://", "postgresql://", 1)

try:
    annotationtable = AnnotationTable(db_url)
except AttributeError:
    annotationtable = None

try:
    maptable = MapTable(db_url)
except AttributeError:
    maptable = None

try:
    scaffoldtable = ScaffoldTable(db_url)
except AttributeError:
    scaffoldtable = None

try:
    featuredDatasetIdSelectorTable = FeaturedDatasetIdSelectorTable(db_url)
except AttributeError:
    featuredDatasetIdSelectorTable = None

try:
    protocolMetricsTable = ProtocolMetricsTable(db_url)
except AttributeError:
    protocolMetricsTable = None


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


@app.before_first_request
def connect_to_pennsieve2():
    global ps2
    try:
        ps2 = new_client(api_key=Config.PENNSIEVE_API_TOKEN, api_secret=Config.PENNSIEVE_API_SECRET, api_host=Config.PENNSIEVE_API_HOST, api2_host=None)
    except Exception as err:
        logging.error(f"Error connecting to pennsieve 2 agent: {err}")


viewers_scheduler = BackgroundScheduler()
metrics_scheduler = BackgroundScheduler()
services_scheduler = BackgroundScheduler()
featured_dataset_id_scheduler = BackgroundScheduler()
update_contentful_event_entries_scheduler = BackgroundScheduler()
protocol_metrics_scheduler = BackgroundScheduler()

# If nothing is stored in the DB than update it now
protocol_metrics = get_protocol_metrics_table_state(protocolMetricsTable)
if Config.SPARC_API_DEBUGGING == 'FALSE' and (protocol_metrics is None or protocol_metrics.get('total_protocol_views') == -1):
    update_protocol_metrics()

if not protocol_metrics_scheduler.running:
    logging.info('Starting scheduler for protocol metrics acquisition')
    protocol_metrics_scheduler.start()

if not featured_dataset_id_scheduler.running:
    logging.info('Starting scheduler for featured dataset id acquisition')
    featured_dataset_id_scheduler.start()

# Run monthly annotation states clean up
if annotationtable:
    annotation_cleanup_scheduler = BackgroundScheduler()
    annotation_cleanup_scheduler.start()
    # Check on the second of each month at 2am
    annotation_cleanup_scheduler.add_job(annotationtable.removeExpiredState, 'cron',
                                         year='*', month='*', day='2', hour='2', minute=0, second=0)

# Only need to run the update contentful entries scheduler on one environment, so dev was chosen to keep prod more responsive
if Config.DEPLOY_ENV == 'development' and Config.SPARC_API_DEBUGGING == 'FALSE':
    if not update_contentful_event_entries_scheduler.running:
        logging.info('Starting scheduler for updating contentful event entries')
        update_contentful_event_entries_scheduler.start()
    # Update the contentful entries daily at 2 AM EST
    update_contentful_event_entries_scheduler.add_job(update_all_events_sort_order, 'cron', hour=2, timezone='US/Eastern')

osparc_data = {}

@app.before_first_request
def get_osparc_file_viewers():
    logging.info('Getting oSPARC viewers')
    # Gets a list of default viewers.
    try:
        req = requests.get(url=f'{Config.OSPARC_API_HOST}/viewers')
        if req.ok and 'application/json' in req.headers.get('content-type', ''):
            viewers = req.json()
            table = build_filetypes_table(viewers["data"])
            osparc_data["file_viewers"] = table
    except Exception as e:
        logging.error('Could not retreive oSPARC viewers', e)
    if not viewers_scheduler.running:
        logging.info('Starting scheduler for oSPARC viewers acquisition')
        viewers_scheduler.start()


usage_metrics = {}
google_analytics = init_ga_reporting()
algolia = init_algolia_client()
contentful = init_cf_cda_client()


@app.before_first_request
def get_metrics():
    logging.info('Gathering metrics data')

    if google_analytics:
        ga_response = get_ga_1year_sessions(google_analytics)
        usage_metrics['1year_sessions_count'] = ga_response

    if algolia:
        algolia_response = get_dataset_count(algolia)
        usage_metrics['dataset_count'] = algolia_response

    if contentful:
        cf_response = get_funded_projects_count(contentful)
        usage_metrics['funded_projects_count'] = cf_response

    ps_response = get_download_count()
    usage_metrics['1year_download_count'] = ps_response

    if not metrics_scheduler.running:
        logging.info('Starting scheduler for metrics acquisition')
        metrics_scheduler.start()


osparc_services = OSparcServices()


@app.before_first_request
def get_services():
    logging.info('Fetching oSPARC services')
    try:
        req = requests.get(url=f'{Config.OSPARC_API_HOST}/services')
        services_resp = req.json()
        osparc_services.set_services(services_resp['data'])
    except Exception as e:
        logging.error('Request to get oSPARC services failed', e)


# Gets oSPARC services before the first request after startup and then once a day.
services_scheduler.add_job(func=get_services, trigger="interval", days=1)

# Gets oSPARC viewers before the first request after startup and then once a day.
viewers_scheduler.add_job(func=get_osparc_file_viewers, trigger="interval", days=1)

# Gathers all the required metrics, once every three hours
metrics_scheduler.add_job(func=get_metrics, trigger='interval', hours=3)

# Update the featured dataset id on deploy and then every hour
featured_dataset_id_trigger = OrTrigger([DateTrigger(), IntervalTrigger(hours=1)])
featured_dataset_id_scheduler.add_job(lambda: set_featured_dataset_id(featuredDatasetIdSelectorTable), featured_dataset_id_trigger)

# Update the protocol metrics once a week on saturday at midnight
if Config.SPARC_API_DEBUGGING == 'FALSE':
    protocol_metrics_scheduler.add_job(update_protocol_metrics, 'cron', day_of_week='sat', hour=0, minute=0)

def shutdown_schedulers():
    logging.info('Stopping scheduler for oSPARC viewers acquisition')
    if viewers_scheduler.running:
        viewers_scheduler.shutdown()
    logging.info('Stopping scheduler for metrics acquisition')
    if metrics_scheduler.running:
        metrics_scheduler.shutdown()
    logging.info('Stopping scheduler for updating contentful entries')
    if update_contentful_event_entries_scheduler.running:
        update_contentful_event_entries_scheduler.shutdown()
    logging.info('Stopping scheduler for updating featured dataset id')
    if featured_dataset_id_scheduler.running:
        featured_dataset_id_scheduler.shutdown()
    logging.info('Stopping scheduler for updating protocol metrics')
    if protocol_metrics_scheduler.running:
        protocol_metrics_scheduler.shutdown()
    logging.info('Stopping scheduler for oSPARC services')
    if services_scheduler.running:
        services_scheduler.shutdown()


atexit.register(shutdown_schedulers)


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
    email_sender.mailersend_email(Config.SES_SENDER, email, 'Feedback submission', feedback_email.substitute({'message': message}))

    return json.dumps({"status": "sent"})


def create_s3_presigned_url(s3BucketName, key, content_type, expiration):
    response = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": s3BucketName, "Key": key, "RequestPayer": "requester", "ResponseContentType": content_type},
        ExpiresIn=expiration,
    )

    return response


# Download a file from S3
@app.route("/download")
def create_presigned_url(expiration=3600, bucket_name=Config.DEFAULT_S3_BUCKET_NAME):
    key = request.args.get("key")
    s3BucketName = request.args.get("s3BucketName", bucket_name)
    content_type = request.args.get("contentType", "application/octet-stream")

    return create_s3_presigned_url(s3BucketName, key, content_type, expiration)


@app.route("/thumbnail/neurolucida")
def thumbnail_from_neurolucida_file():
    query_args = request.args
    if 'version' not in query_args or 'datasetId' not in query_args or 'path' not in query_args:
        return abort(400, description=f"Query arguments are not valid.")

    url = f"{Config.NEUROLUCIDA_HOST}/thumbnail"
    try:
        response = requests.get(url, params=query_args, timeout=5)
        response.raise_for_status()
        if response.status_code == 200:
            if response.headers.get('Content-Type', 'unknown') == 'image/png':
                return base64.b64encode(response.content)
        abort(400, 'Failed to retrieve thumbnail.')

    except requests.exceptions.ConnectionError:
        return abort(400, description="Unable to make a connection to NEUROLUCIDA_HOST.")
    except requests.exceptions.Timeout:
        return abort(504, 'Request to NEUROLUCIDA_HOST timed out.')
    except requests.exceptions.RequestException as e:
        return abort(502, f"Error while requesting NEUROLUCIDA_HOST: {str(e)}")


@app.route("/thumbnail/segmentation")
def extract_thumbnail_from_xml_file(bucket_name=Config.DEFAULT_S3_BUCKET_NAME):
    """
    Extract a thumbnail from a mbf xml file.
    First phase is to find the thumbnail element in the xml document.
    Second phase is to convert the xml to a base64 png.
    """
    query_args = request.args
    if 'path' not in query_args:
        return abort(400, description=f"Query arguments are not valid.")

    s3BucketName = query_args.get("s3BucketName", bucket_name)
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
                Bucket=s3BucketName,
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
def url_exists(path, bucket_name=Config.DEFAULT_S3_BUCKET_NAME):
    query_args = request.args
    s3BucketName = query_args.get("s3BucketName", bucket_name)

    try:
        head_response = s3.head_object(
            Bucket=s3BucketName,
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

    try:
        json_response = fetch_discover_file_information(uri)
        if 'totalCount' in json_response and json_response['totalCount'] == 1:
            file_info = json_response['files'][0]
            return file_info['path']
    except Exception as ex:
        logging.error('Failed to retrieve uri {uri}', ex)
    return abort(404, description=f'Failed to retrieve uri {uri}')


def s3_header_check(path, bucket_name):
    try:
        head_response = s3.head_object(
            Bucket=bucket_name,
            Key=path,
            RequestPayer="requester"
        )
        content_length = head_response.get('ContentLength', Config.DIRECT_DOWNLOAD_LIMIT)
        if content_length and not content_length < Config.DIRECT_DOWNLOAD_LIMIT:  # 20 MB
            return abort(413, description=f"File too big to download: {content_length}")
    except botocore.exceptions.ClientError as err:
        # NOTE: This case is required because of https://github.com/boto/boto3/issues/2442
        if err.response["Error"]["Code"] == "404":
            return (404, f'Provided path was not found on the s3 resource')
        elif err.response["Error"]["Code"] == "403":
            return (403, f'There is a permission issue when accessing the file at specified path')
        else:
            return abort(err.response["Error"]["Code"], err.response["Error"]["Message"])
    else:
        return (200, 'OK')


# Reverse proxy for objects from S3, a simple get object
# operation. This is used by scaffoldvuer and its
# important to keep the relative <path> for accessing
# other required files.
@app.route("/s3-resource/<path:path>")
def direct_download_url(path, bucket_name=Config.DEFAULT_S3_BUCKET_NAME):
    query_args = request.args
    s3BucketName = query_args.get("s3BucketName", bucket_name)
    s3_path = path  # Will modify s3_path if we find name mangling

    # Check the header to see if too large or does not exist
    response = s3_header_check(path, s3BucketName)

    # If the file does not exist, check if the name was mangled
    if response[0] == 404 or response[0] == 403:
        s3_path_modified = get_path_from_mangled_list(path)
        if s3_path_modified == s3_path:
            abort(404, description=f'Provided path was not found on the s3 resource')  # Abort if path did not change

        # Check the modified path
        response2 = s3_header_check(s3_path_modified, s3BucketName)
        if response2[0] == 200:
            s3_path = s3_path_modified  # Modify the path if de-mangling was successful
        elif response2[0] == 404:
            abort(404, description=f'Provided path was not found on the s3 resource')
        elif response2[0] == 403:
            abort(403, description=f'There is a permission issue when accessing the file at specified path')

    response = s3.get_object(
        Bucket=s3BucketName,
        Key=s3_path,
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


@app.route("/multiple_dataset_info/using_multiple_mimetype")
@app.route("/multiple_dataset_info/using_multiple_mimetype/")
def get_file_info_from_mimetype():
    # q here is a scicrunch query ie: "*jp2*+OR+*vnd.ome.xml*+OR+*jpx*"
    q = request.args.getlist('q')
    query = create_multiple_mimetype_query(q)

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


@app.route("/dataset_info/anatomy")
def get_dataset_info_anatomy():
    identifier = request.args.get('identifier', -1)
    if identifier == -1:
        return abort(404, description=f'Identifier for API call not set.')

    query = create_anatomy_query(identifier)

    return reform_anatomy_results(dataset_search(query))


@app.route("/dataset_info/using_pennsieve_identifier")
def get_dataset_info_pennsieve_identifier():
    identifier = request.args.get('identifier')
    query = create_pennsieve_identifier_query(identifier)

    return reform_dataset_results(dataset_search(query))


def get_is_derived_from_with_identifier_or_path(discoverId, version, objects, identifier, matchingPath, datasetCache):
    source_list = []
    for item in objects:
        if ((identifier and item.get("identifier") == identifier) or \
            (matchingPath and item.get('dataset', {}).get('path', '') == matchingPath)):
            datacite = item.get("datacite", {})
            isDerivedFromPaths = datacite.get("isDerivedFrom", {}).get('path')
            derivedfromdataset = item.get("derived_from_dataset", {})
            derivedfromdatasetdoi = derivedfromdataset.get('uri')
            derivedfromdatasetpath = derivedfromdataset.get('path')
            flatmapUUID = item.get("associated_flatmap", {}).get('identifier')
            if flatmapUUID:
                source_list.append(
                    {
                        'discoverId': discoverId,
                        'name': item['name'],
                        'path': item['dataset']['path'],
                        'version': version,
                        'flatmapUUID': flatmapUUID
                    }
                )
            if isDerivedFromPaths is not None:
                for path in isDerivedFromPaths:
                    path = path.replace('derivative/sub-f006/derivative/sub-f006', 'derivative/sub-f006')
                    source_objects = get_is_derived_from_with_identifier_or_path(discoverId, version, objects, None, path, datasetCache)
                    if len(source_objects) > 0:
                        source_list.extend(source_objects)
                    elif derivedfromdatasetdoi is None or derivedfromdatasetpath is None:
                            data = {
                                'discoverId': discoverId,
                                'name': item['name'],
                                'path': item['dataset']['path'],
                                'version': version
                            }
                            source_list.append(data)
            if derivedfromdatasetdoi is not None and derivedfromdatasetpath is not None:
                for i in range(len(derivedfromdatasetdoi)):
                    if 0 <= i < len(derivedfromdatasetpath):
                        doi = derivedfromdatasetdoi[i].replace('https://doi.org/', '')
                        source_list.extend(get_original_source(None, doi, None, derivedfromdatasetpath[i], datasetCache))

    return source_list


# Trace original source all the way till it is found
# or reach an external dataset
def get_original_source_in_dataset(dataset_info, identifier, matchingPath, datasetCache):
    hits = dataset_info.get('hits', {}).get('hits', [])
    #there should only be one result
    if len(hits) == 1:
        objects = hits[0].get('_source', {}).get('objects')
        discoverId = hits[0].get('_source', {}).get('pennsieve', {}).get('identifier')
        version = hits[0].get('_source', {}).get('pennsieve', {}).get('version', {}).get('identifier')
        if objects is not None and discoverId is not None and version is not None:
            return get_is_derived_from_with_identifier_or_path(discoverId, version, objects, identifier, matchingPath, datasetCache)
    return []


def get_original_source(identifier, doi, discoverId, path, datasetCache):
    query = None
    id = identifier
    if identifier is not None:
        query = create_identifier_query(identifier)
    elif path is not None:
        if doi is not None:
            id = doi
            newDOI = doi.replace('DOI:', '')
            query = create_multiple_doi_query([newDOI])
        elif discoverId is not None:
             id = discoverId
             query = create_multiple_discoverId_query([discoverId])
    dataset_info = datasetCache.get(id, None)
    if dataset_info is None:
        dataset_info = dataset_search(query)
        datasetCache[id] = dataset_info
    return get_original_source_in_dataset(dataset_info, identifier, path, datasetCache)


@app.route("/file_info/get_original_source")
def get_file_info_original_source():
    discoverId = request.args.get('discoverId')
    doi = request.args.get('doi')
    identifier = request.args.get('identifier')
    path = request.args.get('path')
    #the following cache the datasetInfo
    datasetCache = {}
    return {'result': get_original_source(identifier, doi, discoverId, path, datasetCache)}


@app.route("/segmentation_info/")
def get_segmentation_info_from_file(bucket_name=Config.DEFAULT_S3_BUCKET_NAME):
    query_args = request.args

    if 'dataset_path' not in query_args:
        return abort(400, description=f"Query arguments must include 'dataset_path'.")

    s3BucketName = query_args.get("s3BucketName", bucket_name)
    dataset_path = query_args.get('dataset_path')

    try:
        # Check the header to see if too large
        response = s3_header_check(dataset_path, s3BucketName)
        # Check if file exists
        if response[0] == 404:
            abort(404, description=f'Provided path was not found on the s3 resource')
        response = s3.get_object(
            Bucket=s3BucketName,
            Key=dataset_path,
            RequestPayer="requester"
        )
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            return abort(404, description=f"Could not find file: '{dataset_path}'")
        else:
            return abort(404, description=f"Unknown error for file: '{dataset_path}'")

    resource = response["Body"].read()
    xml = ElementTree.fromstring(resource)
    subject_element = xml.find('./{*}sparcdata/{*}subject')
    info = {}
    if subject_element is not None:
        info['subject'] = subject_element.attrib
    else:
        info['subject'] = {'age': '', 'sex': '', 'species': '', 'subjectid': ''}

    atlas_element = xml.find('./{*}sparcdata/{*}atlas')
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
        try:
            response = requests.post(
                f'{Config.SCI_CRUNCH_HOST}/_search?api_key={Config.KNOWLEDGEBASE_KEY}',
                json=data)
            json_result = response.json()
            results.append(json_result)
        except json.JSONDecodeError:
            return jsonify({'message': 'Could not parse SciCrunch output, please try again later',
                            'error': 'JSONDecodeError'}), 502
        except Exception as ex:
            logging.error(f"Could not search SciCrunch for path {path}", ex)

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
    uri = resp.get("uri")
    if id_ is None or uri is None:
        return
    parsed_uri = urlparse(uri)
    bucket = parsed_uri.netloc
    try:
        response = s3.get_object(
            Bucket=bucket,
            Key="{}/files/template.json".format(id_),
            RequestPayer="requester",
        )
    except ClientError:
        # If the file is not under folder 'files', check under folder 'packages'
        debugging = Config.SPARC_API_DEBUGGING == "TRUE"
        if debugging:
            logging.warning(
                "Required file template.json was not found under /files folder, trying under /packages..."
            )
        try:
            response = s3.get_object(
                Bucket=bucket,
                Key="{}/packages/template.json".format(id_),
                RequestPayer="requester",
            )
        except ClientError as e:
            if debugging:
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
        try:
            req = requests.get("{}/datasets/{}".format(Config.DISCOVER_API_HOST, id_))
            if req.ok:
                json_data = req.json()
                inject_markdown(json_data)
                inject_template_data(json_data)
                return jsonify(json_data)
        except Exception as ex:
            logging.error(f"Could not fetch SIM dataset {id_}", ex)
        return abort(404, description="Resource not found")


@app.route("/sim/dataset/<id_>/versions/<version_>")
def sim_dataset_versions(id_, version_):
    if request.method == "GET":
        try:
            req = requests.get("{}/datasets/{}/versions/{}".format(Config.DISCOVER_API_HOST, id_, version_))
            if req.ok:
                json_data = req.json()
                inject_markdown(json_data)
                inject_template_data(json_data)
                return jsonify(json_data)
        except Exception as ex:
            logging.error(f"Could not fetch SIM dataset {id_} version {version_}", ex)
        return abort(404, description="Resource not found")


@app.route("/get_osparc_data")
def get_osparc_data():
    return jsonify(osparc_data)


@app.route('/sim/service')
def osparc_search():
    if request.method == 'GET':
        search = request.args.get('search')
        limit = request.args.get('limit', default=5, type=int)
        skip = request.args.get('skip', default=0, type=int)
        results = osparc_services.search_services(search, limit, skip)
        return jsonify(results)


@app.route('/sim/file')
def osparc_extensions():
    if request.method == 'GET':
        extensions = osparc_services.file_extensions
        return jsonify({
            "file_viewers": extensions
        })


@app.route("/project/<project_id>", methods=["GET"])
def datasets_by_project_id(project_id):
    datasets = get_associated_datasets(project_id)

    if len(datasets['hits']) > 0:
        return jsonify(datasets['hits'])
    else:
        abort(404, description="Resource not found")


@app.route("/get_featured_datasets_identifiers", methods=["GET"])
def get_featured_datasets_identifiers():
    return {'identifiers': get_featured_datasets()}


@app.route("/get_featured_dataset", methods=["GET"])
@cache.cached(timeout=300)
def get_featured_dataset():
    featured_dataset_id = get_featured_dataset_id_table_state(featuredDatasetIdSelectorTable)["featured_dataset_id"]
    if featured_dataset_id == -1:
        # In case there was an error while setting the id, just return a default dataset so the homepage does not break.
        featured_dataset_id = 32
    try:
        response = requests.get("{}/datasets?ids={}".format(Config.DISCOVER_API_HOST, featured_dataset_id)).json()
        # in case the dataset has been unpublished, just return default
        if response['datasets'] == []:
            response = requests.get("{}/datasets?ids={}".format(Config.DISCOVER_API_HOST, 32)).json()
        return response
    except Exception as ex:
        logging.error(f"Could not get featured dataset {featured_dataset_id}", ex)
    abort(404, description="An error occured while fetching the resource")


@app.route("/reva/subject-ids", methods=["GET"])
def getRevaSubjectIds():
    try:
        primary_folder = ps2.get(f'/packages/{Config.REVA_3D_TRACING_PRIMARY_FOLDER_COLLECTION_ID}')
        primary_children = primary_folder['children']
        subject_ids = []
        for child in primary_children:
            if child['content']['packageType'] == 'Collection':
                subject_ids.append(child['content']['name'])
        return jsonify({"status": "success", "ids": subject_ids}), 200
    except Exception as e:
        logging.error(f"Error while getting REVA subject id files: {e}")
        return jsonify({"status": "Error while getting REVA subject id files: ", "message": e}), 500


def getRevaTracingInSituFolderChildren(subject_id):
    try:
        coordinates_folder_name = 'CoordinatesData'
        in_situ_folder_name = 'InSitu'
        primary_folder = ps2.get(f'/packages/{Config.REVA_3D_TRACING_PRIMARY_FOLDER_COLLECTION_ID}')
        if not primary_folder:
            msg = f"Primary folder not found: {Config.REVA_3D_TRACING_PRIMARY_FOLDER_COLLECTION_ID}"
            logging.error(msg)
            return abort(404, description=msg)

        primary_children = primary_folder.get('children', [])
        subject_child = next((child for child in primary_children if child['content']['name'] == subject_id), None)
        if subject_child is None:
            msg = f"Subject folder not found for subject id: {subject_id}"
            logging.error(msg)
            return abort(404, description=msg)

        subject_folder = ps2.get(f"/packages/{subject_child['content']['id']}")
        if not subject_folder:
            msg = f"Subject folder could not be fetched for id: {subject_child['content']['id']}"
            logging.error(msg)
            return abort(404, description=msg)

        subject_children = subject_folder.get('children', [])
        coordinates_child = next((child for child in subject_children if child['content']['name'] == coordinates_folder_name), None)
        if coordinates_child is None:
            msg = f"CoordinatesData folder not found for subject: {subject_id}"
            logging.error(msg)
            return abort(404, description=msg)

        coordinates_folder = ps2.get(f"/packages/{coordinates_child['content']['id']}")
        if not coordinates_folder:
            msg = f"CoordinatesData folder could not be fetched for id: {coordinates_child['content']['id']}"
            logging.error(msg)
            return abort(404, description=msg)

        coordinates_children = coordinates_folder.get('children', [])
        in_situ_child = next((child for child in coordinates_children if child['content']['name'] == in_situ_folder_name), None)
        if in_situ_child is None:
            msg = f"InSitu folder not found for subject: {subject_id}"
            logging.error(msg)
            return abort(404, description=msg)

        # Get in situ folder
        in_situ_folder = ps2.get(f"/packages/{in_situ_child['content']['id']}")
        if not in_situ_folder:
            msg = f"InSitu folder could not be fetched for id: {in_situ_child['content']['id']}"
            logging.error(msg)
            return abort(404, description=msg)

        return in_situ_folder.get('children', [])

    except Exception as e:
        msg = f"Exception thrown when getting Reva InSitu Folder: {e}"
        logging.error(msg)
        return abort(500, description=msg)


@app.route("/reva/anatomical-landmarks-files/<subject_id>", methods=["GET"])
def getRevaAnatomicalLandmarksFiles(subject_id):
    try:
        anatomical_landmarks_folder_name = 'AnatomicalLandmarks'
        in_situ_children = getRevaTracingInSituFolderChildren(subject_id)
        anatomical_landmarks_child = next((child for child in in_situ_children if child['content']['name'] == anatomical_landmarks_folder_name), None)
        if anatomical_landmarks_child is None:
            logging.error(f"REVA tracing folder {anatomical_landmarks_folder_name} not found for subject: {subject_id}")
            return jsonify({"status": "ERROR", "message": f"{anatomical_landmarks_folder_name} folder not found for subject: {subject_id}"}), 404
        anatomical_landmarks_folder = ps2.get(f"/packages/{anatomical_landmarks_child['content']['id']}")
        anatomical_landmarks_children = anatomical_landmarks_folder['children']
        anatomical_landmarks_folders = []
        for anatomical_landmark_child in anatomical_landmarks_children:
            landmark_folder_name = anatomical_landmark_child['content']['name']
            landmark_folder_id = anatomical_landmark_child['content']['id']
            anatomical_landmark_folder = ps2.get(f"/packages/{landmark_folder_id}")
            landmark_children = anatomical_landmark_folder['children']
            landmark_files = []
            for landmark_child in landmark_children:
                landmark_file_package_id = landmark_child['content']['id']
                landmark_file = ps2.get(f"/packages/{landmark_file_package_id}/view")
                landmark_file_id = landmark_file[0]['content']['id']
                landmark_file_presigned_url = ps2.get(f"/packages/{landmark_file_package_id}/files/{landmark_file_id}")['url']
                landmark_files.append({'name': str(landmark_child['content']['name']), 's3Url': str(landmark_file_presigned_url)})
            anatomical_landmarks_folders.append({'name': str(landmark_folder_name), 'files': landmark_files})
        return jsonify({"status": "success", "folders": anatomical_landmarks_folders}), 200
    except Exception as e:
        logging.error(f"Error while getting REVA anatomical landmarks files {e}")
        return jsonify({"status": "Error while getting anatomical landmarks files: ", "message": e}), 500


@app.route("/reva/tracing-files/<subject_id>", methods=["GET"])
def getRevaTracingFiles(subject_id):
    try:
        vagus_nerve_folder_name = 'VagusNerve'
        in_situ_children = getRevaTracingInSituFolderChildren(subject_id)
        vagus_nerve_child = next((child for child in in_situ_children if child['content']['name'] == vagus_nerve_folder_name), None)
        if vagus_nerve_child is None:
            logging.error(f"REVA tracing folder {vagus_nerve_folder_name} not found for subject: {subject_id}")
            return jsonify({"status": "ERROR", "message": f"{vagus_nerve_folder_name} folder not found for subject: {subject_id}"}), 404
        vagus_nerve_folder = ps2.get(f"/packages/{vagus_nerve_child['content']['id']}")
        vagus_nerve_children = vagus_nerve_folder['children']
        vagus_tracing_files = []
        for vagus_region_child in vagus_nerve_children:
            vagus_region_folder = ps2.get(f"/packages/{vagus_region_child['content']['id']}")
            # get file and use id and package id for getting the presigned url https://api.pennsieve.io/packages/{id}/view
            vagus_region_children = vagus_region_folder['children']
            for vagus_file_child in vagus_region_children:
                file_package_id = vagus_file_child['content']['id']
                vagus_file = ps2.get(f"/packages/{file_package_id}/view")
                vagus_file_id = vagus_file[0]['content']['id']
                vagus_file_presigned_url = ps2.get(f"/packages/{file_package_id}/files/{vagus_file_id}")['url']
                vagus_tracing_files.append(
                    {'name': str(vagus_file_child['content']['name']), 'region': str(vagus_region_child['content']['name']), 's3Url': str(vagus_file_presigned_url)})
        return jsonify({"status": "success", "files": vagus_tracing_files}), 200
    except Exception as e:
        logging.error(f"Error while getting REVA tracing files {e}")
        return jsonify({"status": "Error while getting tracing files: ", "message": e}), 500


@app.route("/reva/micro-ct-files/<subject_id>", methods=["GET"])
def getRevaMicroCtFiles(subject_id):
    micro_ct_visualization_folder_name = f'{subject_id}-MicroCTVisualization'

    try:
        primary_folder = ps2.get(f'/packages/{Config.REVA_MICRO_CT_PRIMARY_FOLDER_COLLECTION_ID}')
        primary_children = primary_folder['children']
        subject_child = next((child for child in primary_children if child['content']['name'] == subject_id), None)
        if subject_child is None:
            logging.error(f'REVA microCT folder not found with subject id: {subject_id}')
            return jsonify({"status": "ERROR", "message": f"MicroCT folder not found with subject id: {subject_id}"}), 404
        subject_folder = ps2.get(f"/packages/{subject_child['content']['id']}")
        subject_children = subject_folder['children']
        micro_ct_child = next((child for child in subject_children if child['content']['name'] == micro_ct_visualization_folder_name), None)
        if micro_ct_child is None:
            logging.error(f'REVA microCT {micro_ct_visualization_folder_name} folder not found for subject: {subject_id}')
            return jsonify({"status": "ERROR", "message": f"{micro_ct_visualization_folder_name} folder not found for subject: {subject_id}"}), 404
        micro_ct_visualization_folder = ps2.get(f"/packages/{micro_ct_child['content']['id']}")
        micro_ct_children = micro_ct_visualization_folder['children']
        micro_ct_files = []
        for micro_child in micro_ct_children:
            file_package_id = micro_child['content']['id']
            micro_child_file = ps2.get(f"/packages/{file_package_id}/view")
            micro_child_file_id = micro_child_file[0]['content']['id']
            micro_file_presigned_url = ps2.get(f"/packages/{file_package_id}/files/{micro_child_file_id}")['url']
            file_name = micro_child['content']['name']
            file_size = micro_child['storage']
            package_type = micro_child['content']['packageType']
            file_type = micro_child_file[0]['content']['fileType']
            micro_ct_files.append(
                {'name': str(file_name), 's3Url': str(micro_file_presigned_url), 'type': str(file_type), 'packageType': str(package_type), 'size': str(file_size)})
        return jsonify({"status": "success", "files": micro_ct_files}), 200
    except Exception as e:
        logging.error(f"Error while getting REVA microCT files {e}")
        return jsonify({"status": "Error while getting microCT files: ", "message": e}), 500


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


# Get information of the latest body scaffold for species.
# This endpoint returns the metadata file path, bucket,
# dataset id and version which can be used to construct the url
@app.route("/get_body_scaffold_info/<species>", methods=["GET"])
def get_body_scaffold_info(species):
    id = get_body_scaffold_dataset_id(species)
    if id:
        query = create_pennsieve_identifier_query(id)
        result = process_get_first_scaffold_info(dataset_search(query))
        if result:
            return result

    return abort(404, description=f"Whole body info not found for {species}")


@app.route("/thumbnail/<image_id>", methods=["GET"])
def thumbnail_by_image_id(image_id, recursive_call=False):
    bl = Biolucida()

    try:
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
    except Exception as ex:
        logging.error(f"Could not get the thumbnail for {image_id}", ex)
    return abort(404, "An error occured while fetching the thumbnail")


@app.route("/image/<image_id>", methods=["GET"])
def image_info_by_image_id(image_id):
    url = Config.BIOLUCIDA_ENDPOINT + "/image/info/{0}".format(image_id)
    try:
        response = requests.request("GET", url)
        return process_biolucida_result(response.json())
    except Exception as ex:
        logging.error(f"Could not get image info for {image_id}", ex)
    return abort(404, "An error occured while getting the image's info")


@app.route("/image_search/<dataset_id>", methods=["GET"])
def image_search_by_dataset_id(dataset_id):
    url = Config.BIOLUCIDA_ENDPOINT + "/imagemap/search_dataset/discover/{0}".format(dataset_id)
    try:
        response = requests.request("GET", url)
        return response.json()
    except Exception as ex:
        logging.error(f"Could not search images for dataset {dataset_id}", ex)
    return {"error": "An error occured while searching images for dataset"}, 404


@app.route("/image_xmp_info/<image_id>", methods=["GET"])
def image_xmp_info(image_id):
    url = Config.BIOLUCIDA_ENDPOINT + "/image/xmpmetadata/{0}".format(image_id)
    try:
        result = requests.request("GET", url)
    except requests.exceptions.ConnectionError:
        return abort(400, description="Unable to make a connection to Biolucida.")

    response = result.json()
    if response['status'] == 'success':
        return process_biolucida_results(response['data'])

    return abort(400, description=f"XMP info not found for {image_id}")


@app.route("/image_blv_link/<image_id>", methods=["GET"])
def image_blv_link(image_id):
    url = Config.BIOLUCIDA_ENDPOINT + "/image/blv_link/{0}".format(image_id)
    try:
        result = requests.request("GET", url)
    except requests.exceptions.ConnectionError:
        return abort(400, description="Unable to make a connection to Biolucida.")

    response = result.json()
    if response['status'] == 'success':
        return jsonify({'link': response['link']})

    return abort(400, description=f"BLV link not found for {image_id}")


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
    # Commit to database even when testing since the Table re-creates a new session each time to prevent stale sessions
    commit = True
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
@app.route("/annotation/getshareid", methods=["POST"])
def get_annotation_share_link():
    return get_share_link(annotationtable)


# Get the map state using the share link id.
@app.route("/annotation/getstate", methods=["POST"])
def get_annotation_state():
    return get_saved_state(annotationtable)


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


def verify_recaptcha(token):
    try:
        captchaReq = requests.post(
            url=Config.TURNSTILE_URL,
            json={
                "secret": Config.NUXT_TURNSTILE_SECRET_KEY,
                "response": token
            }
        )
        captchaResp = captchaReq.json()
        if "success" not in captchaResp or not captchaResp["success"]:
            return {"error": "Failed Captcha Validation"}, 409
        return captchaResp.get('success', False)
    except Exception as ex:
        logging.error("Could not validate captcha, bypassing validation", ex)


def create_github_issue(title, body, labels=None, assignees=None):
    url = f"https://api.github.com/repos/{Config.SPARC_GITHUB_ORG}/{Config.SPARC_ISSUES_GITHUB_REPO}/issues"
    headers = {
        "Authorization": f"token {Config.SPARC_TECH_LEADS_GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json"
    }

    data = {
        "title": title,
        "body": body,
    }

    if labels:
        data["labels"] = labels

    if assignees:
        data["assignees"] = assignees

    response = requests.post(url, json=data, headers=headers)

    if response.status_code == 201:
        response_json = response.json()
        return {
            "html_url": response_json["html_url"],
            "comments_url": response_json["comments_url"],
            "issue_api_url": response_json["url"]
        }
    else:
        raise Exception(f"GitHub Issue creation failed: {response.text}")


@app.route("/create_issue", methods=["POST"])
def create_issue():
    form = request.form
    recaptcha_token = request.form.get('captcha_token')
    if not app.config['TESTING'] and (not recaptcha_token or not verify_recaptcha(recaptcha_token)):
        return jsonify({'error': 'Invalid reCAPTCHA'}), 400

    task_type = form.get("type", "bug")
    title = form.get("title")
    issue_body = form.get("body")
    if not title or not issue_body:
        abort(400, description="Missing title or body")
    email = form.get("email", "").strip()
    if task_type in ["bug", "feedback", "test"]:
        try:
            issue = create_github_issue(title.strip(), issue_body, labels=[task_type], assignees=Config.GITHUB_ISSUE_ASSIGNEES)
            issue_url = issue['html_url']
            comments_url = issue['comments_url']
            issue_api_url = issue['issue_api_url']
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    else:
        return jsonify({"error": f"Unsupported task type: {task_type}"}), 400

    # default to this if there is no issue_url
    response_message = 'Submission could not be created'
    status_code = 500
    response_status = 'error'
    if (issue_url):
        response_message = 'Submission created successfully. '
        status_code = 201
        response_status = 'success'
        files = request.files
        # host the file on the dummy sparc repo and add the viewable url as a comment to the newly created ticket
        if files and 'attachment' in files:
            attachment = files['attachment']
            file_content = attachment.read()
            file_name = attachment.filename

            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            unique_id = uuid.uuid4().hex
            unique_filename = f"{timestamp}_{unique_id}_{file_name}"

            url = f"https://api.github.com/repos/{Config.SPARC_GITHUB_ORG}/{Config.SPARC_ISSUES_GITHUB_REPO}/contents/attachments/{unique_filename}"
            headers = {
                "Authorization": f"token {Config.SPARC_TECH_LEADS_GITHUB_TOKEN}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28"
            }

            encoded_content = base64.b64encode(file_content).decode('utf-8')

            data = {
                "message": f"Add file {unique_filename}",
                "content": encoded_content
            }
            try:
                response = requests.put(url, headers=headers, json=data)
                if response.status_code in (200, 201):
                    json_response = response.json()
                    image_url = json_response["content"]["download_url"]
                    comment_body = f"![Issue Attachment]({image_url})"
                    headers = {
                        "Authorization": f"token {Config.SPARC_TECH_LEADS_GITHUB_TOKEN}",
                        "Accept": "application/vnd.github+json"
                    }

                    data = {
                        "body": comment_body
                    }

                    response = requests.post(comments_url, json=data, headers=headers)

                    if response.status_code != 201:
                        response_message += 'File attachment unsuccessful. '
                        status_code = 201
                        response_status = 'warning'
                    else:
                        response_message += 'File attachment successful. '
                else:
                    response_message += 'File upload unsuccessful. '
                    status_code = 201
                    response_status = 'warning'
            except Exception as e:
                response_message += 'File upload unsuccessful. '
                status_code = 201
                response_status = 'warning'
        if email:
            # default to bug form if task type not specified
            subject = 'SPARC Reported Issue Submission'
            email_body = issue_reporting_email.substitute({'message': issue_body})
            if (task_type == "feedback"):
                subject = 'SPARC Reported Feedback Submission'
                email_body = feedback_email.substitute({'message': issue_body})
            html_body = markdown.markdown(email_body)
            try:
                email_sender.mailersend_email(Config.SES_SENDER, email, subject, html_body)
                response_message += 'Confirmation email sent to user successful. '
            except Exception as e:
                response_message += 'Confirmation email sent to user unsuccessful. '
                status_code = 201
                response_status = 'warning'
    return jsonify({"message": response_message, "url": issue_url, "issue_api_url": issue_api_url, "status": response_status}), status_code

def get_hubspot_contact(email, firstname, lastname):
    search_url = f"{Config.HUBSPOT_V3_API}/objects/contacts/search"
    search_body = {
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": "email",
                        "operator": "EQ",
                        "value": email
                    }
                ]
            }
        ],
        "properties": ["email"],
        "limit": 1
    }
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + Config.HUBSPOT_API_TOKEN
    }
    
    search_results = requests.post(search_url, headers=headers, json=search_body)
    search_data = search_results.json()
    contact_id = None

    if search_data.get("results"):
        contact_id = search_data["results"][0]["id"]
    else:
        # Create contact if not found
        create_contact_url = f"{Config.HUBSPOT_V3_API}/objects/contacts"
        contact_body = {
            "properties": {
                "email": email,
                "firstname": firstname,
                "lastname": lastname
            }
        }
        create_res = requests.post(create_contact_url, headers=headers, json=contact_body)
        if not create_res.ok:
            raise Exception(f"Hubspot contact creation failed: {create_res.status_code} {create_res.text}")
        contact_id = create_res.json()["id"]
    return contact_id

def create_hubspot_deal(name, stage, pipeline, lead_source=None):
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + Config.HUBSPOT_API_TOKEN
    }
    create_deal_url = f"{Config.HUBSPOT_V3_API}/objects/deals"
    deal_body = {
        "properties": {
            "dealname": name,
            "dealstage": stage,
            "pipeline": pipeline,
            "lead_source_in_deal": lead_source
        }
    }

    deal_res = requests.post(create_deal_url, headers=headers, json=deal_body)
    if not deal_res.ok:
        raise Exception(f"Hubspot deal creation failed: {deal_res.status_code} {deal_res.text}")
    deal_id = deal_res.json()["id"]
    return deal_id

def create_hubspot_note(body, deal_id, contact_id):
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + Config.HUBSPOT_API_TOKEN
    }
    note_url = f"{Config.HUBSPOT_V3_API}/objects/notes"
    hs_timestamp = int(datetime.utcnow().timestamp() * 1000)
    note_payload = {
        "properties": {
            "hs_note_body": body,
            "hs_timestamp": hs_timestamp
        }
    }

    note_res = requests.post(note_url, headers=headers, json=note_payload)
    if not note_res.ok:
        raise Exception(f"HubSpot note creation failed: {note_res.status_code} {note_res.text}")

    note_id = note_res.json()["id"]

    # Step 2: Associate the note to deal
    associate_note_to_deal_url = f"{Config.HUBSPOT_V3_API}/objects/notes/{note_id}/associations/deals/{deal_id}/note_to_deal"
    associate_res_deal = requests.put(associate_note_to_deal_url, headers=headers)
    if not associate_res_deal.ok:
        raise Exception(f"Failed to associate note to deal: {associate_res_deal.status_code} {associate_res_deal.text}")

    # Step 3: Associate the note to contact
    associate_note_to_contact_url = f"{Config.HUBSPOT_V3_API}/objects/notes/{note_id}/associations/contacts/{contact_id}/note_to_contact"
    associate_res_contact = requests.put(associate_note_to_contact_url, headers=headers)
    if not associate_res_contact.ok:
        raise Exception(f"Failed to associate note to contact: {associate_res_contact.status_code} {associate_res_contact.text}")

    return note_id

def associate_hubspot_deal_with_contact(deal_id, contact_id):
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + Config.HUBSPOT_API_TOKEN
    }
    associate_url = f"{Config.HUBSPOT_V3_API}/objects/deals/{deal_id}/associations/contacts/{contact_id}/deal_to_contact"
    assoc_res = requests.put(associate_url, headers=headers)

    if not assoc_res.ok:
        raise Exception(f"HubSpot deal to contact association failed: {assoc_res.status_code} {assoc_res.text}")

    return assoc_res.json()

@app.route("/submit_data_inquiry", methods=["POST"])
def submit_data_inquiry():
    form = request.form
    recaptcha_token = request.form.get('captcha_token')
    if not app.config['TESTING'] and (not recaptcha_token or not verify_recaptcha(recaptcha_token)):
        return jsonify({'error': 'Invalid reCAPTCHA'}), 400
    email = form.get("email", "").strip()
    firstname = form.get("firstname", "").strip()
    lastname = form.get("lastname", "").strip()
    task_type = form.get("type", "")
    is_anbc_form = form.get("isAnbcForm", "false")
    title = form.get("title").strip()
    body = form.get("body").strip()
    is_service_form = form.get("isServiceForm", "false")
    if not title or not body or not email or not firstname or not lastname:
        return jsonify({"error": "Missing title, body, email, first name, or last name"}), 400
    if task_type not in ["research","interest"]:
        return jsonify({"error": f"Unsupported task type: {task_type}"}), 400

    contact_id = None
    deal_id = None
    note_id = None
    deal_pipeline = Config.HUBSPOT_ONBOARDING_PIPELINE_ID if task_type == "research" else Config.HUBSPOT_GRANT_SEEKER_PIPELINE_ID
    deal_stage = Config.HUBSPOT_ONBOARDING_PIPELINE_INITIAL_STAGE_ID if task_type == "research" else Config.HUBSPOT_GRANT_SEEKER_PIPELINE_INITIAL_STAGE_ID
    deal_lead_source = Config.ANBC_LEAD_SOURCE if is_anbc_form == 'true' else None
    partial_success = {}
    try:
        contact_id = get_hubspot_contact(email, firstname, lastname)
    except Exception as e:
        return jsonify({
            "error": "Failed to create or retrieve contact. ",
            "details": str(e)
        }), 500

    try:
        deal_id = create_hubspot_deal(title, deal_stage, deal_pipeline, deal_lead_source)
    except Exception as e:
        return jsonify({
            "error": "Failed to create deal. ",
            "contact_id": contact_id,
            "details": str(e)
        }), 500

    try:
        associate_hubspot_deal_with_contact(deal_id, contact_id)
    except Exception as e:
        return jsonify({
            "error": "Failed to associate deal with contact. ",
            "contact_id": contact_id,
            "deal_id": deal_id,
            "details": str(e)
        }), 500

    try:
        # Create a note containing the form body and associate it to the contact and deal
        note_id = create_hubspot_note(body, deal_id, contact_id)
    except Exception as e:
        # Don't fail the whole submission — just inform the user
        partial_success = {
            "warning": "Request successfully submitted, but note creation failed. ",
            "contact_id": contact_id,
            "deal_id": deal_id,
            "details": str(e)
        }

    response = {
        "message": "Request successfully submitted. ",
        "status": "success",
        "contact_id": contact_id,
        "deal_id": deal_id,
        "note_id": note_id
    }

    if email:
        subject = 'SPARC Form Submission Confirmation'
        email_body = ''
        if is_service_form == 'true':
            email_body = service_form_submission_request_confirmation_email.substitute({'name': firstname, 'message': body})
        else:
            email_body = anbc_form_creation_request_confirmation_email.substitute({'name': firstname, 'message': body}) if is_anbc_form == 'true' else creation_request_confirmation_email.substitute({'name': firstname, 'message': body})
        html_body = markdown.markdown(email_body)
        try:
            email_sender.mailersend_email(Config.SES_SENDER, email, subject, html_body, cc=Config.SERVICES_EMAIL)
            response['message'] = response.get('message', '') + 'Confirmation email sent to user successfully. '
            if partial_success:
                partial_success['warning'] = partial_success.get('warning', '') + 'Confirmation email sent to user successfully. '
        except Exception as e:
            if partial_success:
                partial_success['warning'] = partial_success.get('warning', '') + 'Confirmation email sent to user unsuccessful. '
                partial_success['details'] = partial_success.get('details', '') + str(e)
            else:
                partial_success = {
                    "warning": "Request successfully submitted, but confirmation email sent to user unsuccessful.",
                    "contact_id": contact_id,
                    "deal_id": deal_id,
                    "details": str(e)
                }

    if partial_success:
        response.update(partial_success)
        return jsonify(response), 207

    return jsonify(response), 201

@app.route("/tasks", methods=["POST"])
def report_form_submission():
    form = request.form
    if "captcha_token" in form:
        try:
            captchaReq = requests.post(
                url=Config.TURNSTILE_URL,
                json={
                    "secret": Config.NUXT_TURNSTILE_SECRET_KEY,
                    "response": form["captcha_token"]
                }
            )
            captchaResp = captchaReq.json()
            if "success" not in captchaResp or not captchaResp["success"]:
                return {"error": "Failed Captcha Validation"}, 409
        except Exception as ex:
            logging.error("Could not validate captcha, bypassing validation", ex)
    elif not app.config['TESTING']:
        return {"error": "Failed Captcha Validation"}, 409
    
    # Captcha all good
    has_attachment = False
    image_id = uuid.uuid4()
    file = None
    file_upload_response = None
    request_response = {
      "message": "Request successfully submitted. ",
      "status": "success",
      "attachment_filename": ""
    }
    request_response_code = 201

    # --- Handle attachment upload ---
    if 'attachment' in request.files:
        try:
            has_attachment = True
            file = request.files["attachment"]
            drive_client = init_drive_client()
            file_upload_response = upload_file(drive_client, file, str(image_id) + get_extension(file.filename))
        except Exception as e:
            request_response['message'] += "Failed to upload attachment file. "
            request_response['status'] = 'warning'
            request_response_code = 207
            print(f"[ERROR] Failed to upload task attachment: {e}")
    # --- Build description ---
    description = form["description"]
    if has_attachment and file_upload_response and file_upload_response['webViewLink']:
        description += "\n\nAttachment: " + file_upload_response['webViewLink']
        request_response["attachment_filename"] = image_id
    description = description.replace("\r\n", "\n").replace("\n", "<br>")
    # --- Save to Google Sheets ---
    try:
        client = init_gspread_client()
        success = append_contact(
            client,
            [form.get("title"), None, None, None, None, None, description]
        )
    except Exception as e:
        print(f"[ERROR] Failed to add task to Google Sheets: {e}")
        return {"message": "Request submission failed", "status": 'failure'}, 500
    if not success:
        return {"message": "Request submission failed", "status": 'failure'}, 500
    # --- Send confirmation email ---
    try:
        user_email = form.get("userEmail")
        if user_email:
            name = form.get("firstName") or user_email
            subject = 'SPARC Submission'
            body = creation_request_confirmation_email.substitute({
                'name': name,
                'message': f"<div>{description}</div>"
            })

            task_type = form.get("type", "").strip()
            if task_type == "news":
                subject = 'SPARC News Submission'
            elif task_type == "event":
                subject = 'SPARC Event Submission'
            elif task_type == "toolsAndResources":
                subject = 'SPARC Tool/Resource Submission'
            elif task_type == "communitySpotlight":
                subject = 'SPARC Story Submission'
            if len(user_email) > 0 and subject and body:
                if (file):
                    file_bytes = file.read()
                    encoded_file = base64.b64encode(file_bytes).decode("utf-8")
                    email_sender.mailersend_email_with_attachment(Config.SES_SENDER, user_email, subject, body, encoded_file, file.filename, file.mimetype)
                else:
                    email_sender.mailersend_email(Config.SES_SENDER, user_email, subject, body)
    except Exception as e:
        request_response['message'] += "Confirmation email sent to user unsuccessful. "
        request_response['status'] = 'warning'
        request_response_code = 207
        print(f"[ERROR] Failed to send confirmation email: {e}")
        # Don't fail the whole request if email sending fails
    return jsonify(request_response), request_response_code


@app.route("/hubspot_contact_properties/<email>", methods=["GET"])
def get_hubspot_contact_properties(email):
    url = f"{Config.HUBSPOT_V3_API}/objects/contacts/{email}?archived=false&idProperty=email&properties=firstname,lastname,email,newsletter,event_name"
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + Config.HUBSPOT_API_TOKEN
    }
    try:
        response = requests.get(url, headers=headers)
        # Handle successful responses (2xx)
        if response.status_code == 200:
            return jsonify(response.json())
        # Handle not found (404)
        elif response.status_code == 404:
            return jsonify({
                "error": "Contact not found",
                "message": f"No contact with the email '{email}' was found in HubSpot."
            }), 404
        # Handle other non-success status codes
        else:
            return jsonify({
                "error": "Failed to fetch contact",
                "message": f"HubSpot API responded with status code {response.status_code}.",
                "details": response.json() if response.headers.get("Content-Type") == "application/json" else response.text
            }), response.status_code
    except requests.RequestException as ex:
        # Handle exceptions raised by the requests library
        return jsonify({
            "error": "RequestException",
            "message": f"Could not get contact with email '{email}' due to a request error.",
            "details": str(ex)
        }), 500
    except Exception as ex:
        # Handle other unexpected exceptions
        return jsonify({
            "error": "Internal Server Error",
            "message": f"An unexpected error occurred while fetching the contact with email '{email}'.",
            "details": str(ex)
        }), 500


@app.route("/subscribe_to_newsletter", methods=["POST"])
def subscribe_to_newsletter():
    data = request.json
    email = data.get('email_address')
    first_name = data.get('first_name')
    last_name = data.get('last_name')

    # Ensure the required `email` field is present
    if not email:
        return jsonify({'error': 'Email is required'}), 400

    newsletter_property = ''
    try:
        contact_properties, status_code = get_hubspot_contact_properties(email)
        if status_code == 200:
            newsletter_property = contact_properties['properties'].get('newsletter', None)
        else:
            logging.error(f"Unexpected response from HubSpot: {contact_properties}")
            raise Exception(f"Unexpected error: {contact_properties}")
    except Exception as e:
        logging.error(f"Error while retrieving contact properties for email {email}: {e}")

    current_newsletter_values = []
    if isinstance(newsletter_property, str):
        current_newsletter_values = newsletter_property.split(';')
        # remove possible empty string
        current_newsletter_values = list(filter(None, current_newsletter_values))

    # Append the Newsletter value if it's not already in the array
    if 'Newsletter' not in current_newsletter_values:
        current_newsletter_values.append('Newsletter')
    payload = {
        "inputs": [
            {
                "properties": {
                    "email": email,
                    "firstname": first_name,
                    "lastname": last_name,
                    "newsletter": ';'.join(current_newsletter_values)
                },
                "id": email,
                "idProperty": "email"
            }
        ]
    }
    url = f"{Config.HUBSPOT_V3_API}/objects/contacts/batch/upsert"
    headers = {
        "Content-Type": "application/json",
        'Authorization': 'Bearer ' + Config.HUBSPOT_API_TOKEN
    }

    # Send request to HubSpot API
    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
        return jsonify(response.json()), 200
    except requests.exceptions.RequestException as e:
        return jsonify({'error': str(e)}), 500


def get_contact_properties(object_id):
    client = hubspot.Client.create(access_token=Config.HUBSPOT_API_TOKEN)
    try:
        contact_data = client.crm.contacts.basic_api.get_by_id(contact_id=str(object_id), properties_with_history=["firstname", "lastname", "email", "newsletter", "event_name"],
                                                               archived=False)
    except ApiException as e:
        return abort(400, description=f"Exception thrown when getting contact properties: {e}")

    if not contact_data:
        return abort(400, description="Failed to retrieve contact data from HubSpot.")
    if not contact_data.properties_with_history:
        return abort(400, description="Contact properties not found")
    if not contact_data.properties_with_history.get("email"):
        return abort(400, description="Contact Email property not found")
    email = contact_data.properties_with_history.get("email")[0].value
    firstname_data = contact_data.properties_with_history.get("firstname", [{}])[0]
    firstname = firstname_data.value if firstname_data else ""
    lastname_data = contact_data.properties_with_history.get("lastname", [{}])[0]
    lastname = lastname_data.value if lastname_data else ""
    # The newsletter array contains tags where each one corresponds to a mailing list in EmailOctopus that a user can opt-in/out of  
    newsletter_tags_data = contact_data.properties_with_history.get("newsletter")
    if len(newsletter_tags_data) > 0:
        newsletter_tags_data = newsletter_tags_data[0]
    newsletter_tags = newsletter_tags_data.value.split(";") if newsletter_tags_data else []
    # The events array contains tags where each one corresponds to a mailing list in EmailOctopus that a user cannot opt-in/out of
    events_tags_data = contact_data.properties_with_history.get("event_name")
    if len(events_tags_data) > 0:
        events_tags_data = events_tags_data[0]
    events_tags = events_tags_data.value.split(";") if events_tags_data else []
    # Filter out empty strings from the combined list
    tags = [tag for tag in (newsletter_tags + events_tags) if tag]
    return {
        'email': email,
        'firstname': firstname,
        'lastname': lastname,
        'tags': tags
    }


def add_or_update_emailoctopus_contact(list_id, email, firstname, lastname, tags, status):
    url = f"https://api.emailoctopus.com/lists/{list_id}/contacts"
    headers = {
        "Content-Type": "application/json",
        'Authorization': 'Bearer ' + Config.EMAIL_OCTOPUS_API_KEY
    }
    payload = {
        "email_address": email,
        "fields": {"FirstName": firstname, "LastName": lastname},
        "status": status,
        "tags": tags
    }
    try:
        response = requests.put(url, json=payload, headers=headers)
        if str(response.status_code) != '200':
            logging.error(f'Emailoctopus contact did not get added/updated for email: {email}. Returned a response of {response.status_code}: {response.text}')
        return response.json()
    except Exception as ex:
        logging.error(f"Could not add or update contact with email address: {email} in emailoctopus list: {list_id}", ex)
        return abort(500, description=f"Could not add/update contact with email address: {email} from emailoctopus list with ID: {list_id} due to the following error: {ex}")


@app.route("/hubspot_webhook", methods=["POST"])
def hubspot_webhook():
    body = None
    try:
        body = request.get_json(force=True)
    except Exception as e:
        logging.error(f"Invalid JSON body: {e}")
        return jsonify({"error": "Invalid JSON format"}), 400

    if not isinstance(body, list) or not body:
        logging.error(f'Expected an array of webhook events: {body}')
        return jsonify({"error": "Expected a non-empty JSON array"}), 400

    app.logger.info(f'Received Hubspot webhook request: {request}')
    app.logger.info(f'Hubspot webhook request body: {body}')
    if 'X-HubSpot-Request-Timestamp' not in request.headers or 'X-HubSpot-Signature-V3' not in request.headers:
        logging.error(f'Required signature header(s) not present in the following request headers: {request.headers}')
        return jsonify({"error": f"Required signature header(s) not present in the following request headers: {request.headers}"}), 400
    signature_header = request.headers.get("X-HubSpot-Signature-V3")
    timestamp_header = request.headers["X-HubSpot-Request-Timestamp"]
    try:
        signature_timestamp = int(timestamp_header)
    except ValueError:
        logging.error(f'Invalid signature timestamp format: {timestamp_header}')
        return jsonify({"error": "Invalid signature timestamp format"}), 400
    try:
        current_time = int(time.time())
        if current_time - signature_timestamp > 300:
            logging.error(f'Signature timestamp is older than 5 minutes: current time = {current_time}, signature time = {signature_timestamp}')
            return jsonify({'error': 'Signature timestamp is older than 5 minutes'}), 400

        # Concatenate request method, URI, body, and header timestamp
        url = request.url
        method = 'POST'
        stringified_body = json.dumps(body, separators=(",", ":"))
        raw_string = f"{method}{url}{stringified_body}{timestamp_header}"

        # Create HMAC SHA-256 hash from the raw string, then base64-encode it
        hashed_signature = hmac.new(
            Config.HUBSPOT_CLIENT_SECRET.encode('utf-8'),
            raw_string.encode('utf-8'),
            hashlib.sha256
        ).digest()

        base64_hashed_signature = base64.b64encode(hashed_signature).decode('utf-8')

        # Validate the signature if we are not running a test
        if not hmac.compare_digest(base64_hashed_signature, signature_header):
            logging.error(f'Signature is invalid')
            return jsonify({"error": "Signature is invalid"}), 401
    except Exception as ex:
        logging.error(f'Internal error when validating Hubspot webhook request signature: {ex}')
        return jsonify({"error": f"Internal error when validating Hubspot webhook request signature: {ex}"}), 500

    # execute this in a separate thread so that we can send the acknowledgement response to HubSpot asap and do not block the api server
    def process_event(event):
        with app.app_context():
            subscription_type = event.get("subscriptionType")
            object_id = event.get("objectId")
            if subscription_type is None or object_id is None:
                logging.error(f"Missing required keys in event: {event}")
                return
            contact_data = None
            try:
                # HubSpot only provides the contact id so we have to request the contact details separately
                contact_data = get_contact_properties(object_id)
            except Exception as ex:
                logging.error(f'Could not retrieve contact information for ID: {object_id} due to the following error: {ex}')
                return
            try:
                firstname = contact_data["firstname"]
                lastname = contact_data["lastname"]
                email = contact_data["email"]
                emailoctopus_contact = add_or_update_emailoctopus_contact(Config.EMAIL_OCTOPUS_MASTER_LIST_ID, email, firstname, lastname, [], 'subscribed')
                if subscription_type == "contact.propertyChange":
                    tags_to_add = []
                    for tag in contact_data["tags"]:
                        if tag not in emailoctopus_contact["tags"]:
                            tags_to_add.append(tag)
                    # Now we must cycle through all the tags in order to see if any must be removed since we don't know what tags were added or removed in hubspot
                    tags_to_remove = []
                    for tag in emailoctopus_contact["tags"]:
                        if tag not in contact_data["tags"]:
                            tags_to_remove.append(tag)
                    updated_contact_tags = {tag: True for tag in tags_to_add}
                    updated_contact_tags.update({tag: False for tag in tags_to_remove})
                    add_or_update_emailoctopus_contact(Config.EMAIL_OCTOPUS_MASTER_LIST_ID, email, firstname, lastname, updated_contact_tags, 'subscribed')
                else:
                    logging.error(f'Unsupported subscription type: {subscription_type}')
            except Exception as ex:
                logging.error(f"Error processing event {event}: {ex}")

    for event in body:
        if not isinstance(event, dict):
            logging.warning(f"Skipping non-dict event: {event}")
            continue
        executor.submit(process_event, event)

    return jsonify({"status": "success", "message": "Webhook request received and signature verified"}), 200


# Get list of available name / curie pair
@app.route("/get-organ-curies/")
def get_available_uberonids():
    species = request.args.getlist('species')

    requestBody = create_request_body_for_curies(species)

    result = {}

    try:
        response = requests.post(
            f'{Config.SCI_CRUNCH_HOST}/_search?api_key={Config.KNOWLEDGEBASE_KEY}',
            json=requestBody)
        result = reform_curies_results(response.json())
    except BaseException as ex:
        logging.error("Failed getting Uberon IDs", ex)
        return {
            "message": "Could not parse SciCrunch output, please try again later",
            "error": "BaseException"
        }, 502

    return jsonify(result)


# Get list of terms a level up/down from
@app.route("/get-related-terms/<query>")
def get_related_terms(query):
    payload = {
        'direction': request.args.get('direction', default='OUTGOING'),
        'relationshipType': request.args.get('relationshipType', default='BFO:0000050'),
        'entail': request.args.get('entail', default='true'),
        'api_key': Config.KNOWLEDGEBASE_KEY
    }

    result = {}

    try:
        response = requests.get(
            f'{Config.SCI_CRUNCH_SCIGRAPH_HOST}/graph/neighbors/{query}',
            params=payload)
        result = reform_related_terms(response.json())
    except BaseException as ex:
        logging.error(f"Failed getting related terms with payload {payload}", ex)
        return {
            "message": "Could not parse SciCrunch output, please try again later",
            "error": "BaseException"
        }, 502

    return jsonify(result)


@app.route("/simulation_ui_file/<identifier>")
def simulation_ui_file(identifier):
    results = process_results(dataset_search(create_pennsieve_identifier_query(identifier)))
    results_json = json.loads(results.data)

    try:
        item = results_json["results"][0]
        uri = item["s3uri"]
        path = item["abi-simulation-file"][0]["dataset"]["path"]
        key = re.sub(r"s3://[^/]*/", "", f"{uri}files/{path}")
        s3_bucket_name = re.sub(r"s3://|/.*", "", uri)

        return jsonify(json.loads(direct_download_url(key, s3_bucket_name)))
    except Exception:
        abort(404, description="no simulation UI file could be found")


@app.route("/pmr_file", methods=["POST"])
def pmr_file():
    data = request.get_json()
    if data and "path" in data:
        try:
            resp = requests.post(f"{Config.PMR_HOST}/{data['path']}")
            if resp.status_code == 200:
                return base64.b64encode(resp.content)
            else:
                return resp.json()
        except:
            abort(400, description="invalid path")
    else:
        abort(400, description="missing path")


@app.route("/start_simulation", methods=["POST"])
def start_simulation():
    data = request.get_json()

    if data and "solver" in data and "name" in data["solver"] and "version" in data["solver"]:
        return json.dumps(do_start_simulation(data))
    else:
        abort(400, description="Missing solver name and/or solver version")


@app.route("/check_simulation", methods=["POST"])
def check_simulation():
    data = request.get_json()

    if data and "job_id" in data and "solver" in data and "name" in data["solver"] and "version" in data["solver"]:
        return json.dumps(do_check_simulation(data))
    else:
        abort(400, description="Missing solver name, solver version and/or job id")


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

    try:
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
    except Exception as ex:
        logging.error("An error occured while fetching from SciCrunch", ex)
    return abort(500)


@app.route("/dataset_citations/<dataset_id>", methods=["GET"])
def get_dataset_citations(dataset_id):
    headers = {
        'Accept': 'application/json',
    }

    params = {
        "api_key": Config.KNOWLEDGEBASE_KEY
    }

    query = create_citations_query(dataset_id)

    try:
        response = requests.get(f'{Config.SCI_CRUNCH_CITATIONS_HOST}/_search', headers=headers, params=params, json=query)

        results = response.json()
        hits = results['hits']['hits']
        total = results['hits']['total']['value']
        if total == 1:
            result = hits[0]
            json_data = result['_source']
        else:
            json_data = {'dataset id': 'not found'}

        return json_data
    except Exception as ex:
        logging.error("An error occured while fetching from SciCrunch", ex)
    return jsonify({ 'message': f"An error occured while fetching citation info for dataset {dataset_id} from SciCrunch" }), 500

@app.route("/total_dataset_citations", methods=["GET"])
def get_total_dataset_citations():
    headers = {
        'Accept': 'application/json',
    }

    params = {
        "api_key": Config.KNOWLEDGEBASE_KEY
    }

    query = {
        "size": 0,
        "from": 0,
        "query": { "match_all": {} },
        "aggregations": {
            "Citations": {
                "terms": {
                    "field": "citations.type"
                }
            }
        }
    }

    try:
        response = requests.get(f'{Config.SCI_CRUNCH_CITATIONS_HOST}/_search', headers=headers, params=params, json=query)
        results = response.json()
        buckets = results['aggregations']['Citations']['buckets']
        total = sum(bucket["doc_count"] for bucket in buckets)
        return jsonify({ 'total_citations': total }), 200
    except Exception as ex:
        logging.error("An error occured while fetching total citations from SciCrunch", ex)
    return jsonify({ 'total_citations': -1, 'message': "An error occured while fetching total citations from SciCrunch" }), 500

@app.route("/search-readme/<query>", methods=["GET"])
def search_readme(query):
    url = 'https://dash.readme.com/api/v1/docs/search?search=' + query
    headers = {'Authorization': 'Basic ' + Config.README_API_KEY}

    try:
        response = requests.post(
            url=url,
            headers=headers
        )
        return response.json()
    except requests.exceptions.HTTPError as err:
        logging.error(err)
        return {
            "error": str(err),
            "message": "Readme is not currently reachable, please try again later"
        }, 502


@app.route("/metrics", methods=["GET"])
def metrics():
    return usage_metrics


# Callback endpoint for contentful event updated webhook that gets triggered when an event is updated in Contentful
@app.route("/event_updated", methods=["POST"])
def event_updated():
    # the webhook secret key is configured with the same value as the cda access token. If the cda access token is updated then we must update this as well.
    secret_key = request.headers.get('event_updated_secret_key')
    if secret_key != Config.CTF_CDA_ACCESS_TOKEN:
        abort(403, description=f'Invalid secret key: {secret_key}')
    else:
        event = request.get_json()
        if event:
            try:
                return update_event_sort_order(event)
            except:
                abort(400, description=f'Invalid event data: {event}')
        else:
            abort(400, description="Missing event data")

@cache.cached(timeout=86400)
@app.route("/all_dataset_ids", methods=["GET"])
def all_dataset_ids():
    list = get_all_dataset_ids()
    string_list = [str(element) for element in list]
    delimiter = ", "
    return delimiter.join(string_list)

@cache.cached(timeout=86400)
@app.route("/all_dataset_uuids", methods=["GET"])
def all_dataset_uuids():
    list = get_all_dataset_uuids()
    string_list = [str(element) for element in list]
    delimiter = ", "
    return delimiter.join(string_list)

@app.route("/total_protocol_views")
@cache.cached(timeout=180)
def get_total_protocol_views():
    table_state = get_protocol_metrics_table_state(protocolMetricsTable)
    if table_state is None or not table_state.get("total_protocol_views"):
        return jsonify({
            "total_views": None,
            "message": "Total views not yet calculated."
        }), 202
    total_protocol_views = table_state.get("total_protocol_views")

    return jsonify({"total_views": total_protocol_views}), 200

@app.route("/contact_support", methods=["POST"])
def contact_support():
    data = request.get_json()

    name = data.get("name")
    email = data.get("email")
    message = data.get("message")
    subject = data.get("subject", "SPARC Form Submission Confirmation")

    if not name or not email or not message:
        return jsonify({"error": "Missing required fields"}), 400

    if email:
        try:
            email_sender.mailersend_email(Config.SES_SENDER,
                                    email,
                                    subject,
                                    feedback_email.substitute({'message': message}),
                                    Config.SERVICES_EMAIL,
                                    cc=Config.SERVICES_EMAIL)
        except Exception as e:
              return jsonify({"message": "Confirmation email sent to user unsuccessful."}), 500

    return jsonify({"message": "Message received successfully."}), 200
