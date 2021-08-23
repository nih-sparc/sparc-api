import json
import pytest
import threading
from app import app

from concurrent.futures import ThreadPoolExecutor, as_completed
from timeit import default_timer as timer


@pytest.fixture
def client():
    # Spin up test flask app
    app.config['TESTING'] = True
    return app.test_client()


def query_discover_path(client, file_uri):
    r = client.get('/s3-resource/discover_path', query_string={'uri': file_uri})
    response = r.data.decode()
    if '404 Not Found' in response:
        print(response)
    return response


def map_modified_file_names(client, result):

    file_names_map = {}
    if 'pennsieve' not in result:
        if 'status' in result['item']['published'] and result['item']['published']['status'] == 'embargo':
            print('Embargoed dataset')
            return file_names_map
        print('Not a valid dataset response.')
        return file_names_map

    if 'objects' not in result:
        print(f'No objects for dataset?')
        return file_names_map

    with ThreadPoolExecutor(max_workers=8, thread_name_prefix='c3po') as executor:
        # Start the load operations and mark each future with its URL
        future_to_file_path = {executor.submit(query_discover_path, client, obj['distributions']['api'][0]['uri']): obj['dataset']['path'] for obj in result['objects'] if obj['mimetype']['name'] != 'inode/directory'}
        for future in as_completed(future_to_file_path):
            file_path = future_to_file_path[future]
            try:
                data = future.result()
            except Exception as exc:
                print('%r generated an exception: %s' % (file_path, exc))
            else:
                absolute_file_path = f'files/{file_path}'
                if data != absolute_file_path and '404 Not Found' not in data:
                    file_names_map[absolute_file_path] = data

    return file_names_map


def process_dataset(client, doi):
    thread_name = threading.current_thread().name
    raw_response = True
    start = timer()
    r = client.get('/dataset_info/using_doi', query_string={'doi': doi, 'raw_response': raw_response})
    end = timer()
    print(f"elapsed dataset query ({thread_name}):", end - start)
    response = json.loads(r.data)
    assert len(response['hits']['hits']) == 1
    hit = response['hits']['hits'][0]
    start = timer()
    file_names_map = map_modified_file_names(client, hit['_source'])
    end = timer()
    print(f"map file names elapsed ({thread_name}):", end - start)

    return file_names_map


def test_determine_mismatched_file_names(client):
    doi_stem = "doi:"
    print()
    start = timer()
    r = client.get('/current_doi_list')
    end = timer()
    print("elapsed:", end - start)
    response = json.loads(r.data)

    found_doi_s = response['results']

    big_file_names_map = {}

    # We can use a with statement to ensure threads are cleaned up promptly
    with ThreadPoolExecutor(max_workers=10, thread_name_prefix='r2d2') as executor:
        # Start the load operations and mark each future with its URL
        future_to_doi = {executor.submit(process_dataset, client, doi.replace(doi_stem, "")): doi for doi in found_doi_s[11:12]}
        for future in as_completed(future_to_doi):
            doi = future_to_doi[future]
            try:
                data = future.result()
            except Exception as exc:
                print('%r generated an exception: %s' % (doi, exc))
            else:
                big_file_names_map.update(data)

    print(big_file_names_map)