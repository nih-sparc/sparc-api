import json
import pytest
from app import app

from timeit import default_timer as timer

from app.scicrunch_processing_common import SCAFFOLD_FILE, PLOT_FILE, COMMON_IMAGES, THUMBNAIL_IMAGE, NAME, BIOLUCIDA_3D, VIDEO, SEGMENTATION_FILES, BIOLUCIDA_2D
from known_dois import current_list, warn_doi_changes


@pytest.fixture
def client():
    # Spin up test flask app
    app.config['TESTING'] = True
    return app.test_client()


def test_current_doi_list(client):
    doi_stem = "doi:"
    start = timer()
    r = client.get('/current_doi_list')
    end = timer()
    print("elapsed:", end - start)
    response = json.loads(r.data)

    # print(len(response['results']))
    found_doi_s = response['results']

    print("doi_s = [")
    for doi in found_doi_s:
        print(f'    "{doi.replace(doi_stem, "")}",')
    print("]")
    print(f'DOI count: {len(found_doi_s)}')
    unique_doi_s = list(set(found_doi_s))
    assert len(unique_doi_s) == len(found_doi_s)

    changes = []
    for doi in found_doi_s:
        stored_doi = doi.replace(doi_stem, "")
        if stored_doi not in current_list:
            changes.append(doi)

    if len(changes):
        print(f'The current DOI list has changed {len(changes)}.')
        print(changes)

    if len(changes):
        warn_doi_changes()


def print_search_result(result):
    keys = result.keys()

    found = False
    messages = []
    if COMMON_IMAGES in keys:
        found = True
        messages.append(f" - Found common image: {len(result[COMMON_IMAGES])}")
    if BIOLUCIDA_2D in keys:
        found = True
        messages.append(f" - Found Biolucida 2D image: {len(result[BIOLUCIDA_2D])}")
    if BIOLUCIDA_3D in keys:
        found = True
        messages.append(f" - Found Biolucida 3D image: {len(result[BIOLUCIDA_3D])}")
    if SCAFFOLD_FILE in keys:
        found = True
        messages.append(f" - Found scaffold: {len(result[SCAFFOLD_FILE])}")
    if PLOT_FILE in keys:
        found = True
        messages.append(f" - Found plot: {len(result[PLOT_FILE])}")
    if THUMBNAIL_IMAGE in keys:
        found = True
        messages.append(f" - Found scaffold thumbnail: {len(result[THUMBNAIL_IMAGE])}")
    if VIDEO in keys:
        found = True
        messages.append(f" - Found video: {len(result[VIDEO])}")
    if SEGMENTATION_FILES in keys:
        found = True
        messages.append(f" - Found segmentation: {len(result[SEGMENTATION_FILES])}")

    print(result[NAME])
    if found:
        print('\n'.join(messages))


def print_mime_paths(files):
    for file_ in files:
        mimetype = file_.get('mimetype', 'not-specified')
        if mimetype == "inode/directory":
            pass
        elif mimetype == 'not-specified':
            pass
        else:
            print(file_['mimetype'], file_['dataset']['path'])


@pytest.mark.skip(reason="very long test")
def test_all_known_doi_search(client):
    print()
    count = 0
    raw_response = False
    for index, doi in enumerate(current_list):
        count += 1
        if count > 300:
            break
        if index < 46:
            continue
        start = timer()
        print(f'Results for DOI [{index + 1}/{len(current_list)}]: {doi}')
        r = client.get('/dataset_info/using_doi', query_string={'doi': doi, 'raw_response': raw_response})
        end = timer()
        print('elapsed: ', end - start)

        response = json.loads(r.data)

        assert 'result' in response
        if len(response['result']) == 1:
            assert len(response['result']) == 1
            result = response['result'][0]
            print_search_result(result)
        elif len(response['result']) == 0:
            print(f'No result for {doi}!')
        else:
            print(f'Multiple results for {doi}!!')


def test_generic_mouse_colon_dataset_search(client):
    title = "Generic mouse colon scaffold"
    start = timer()
    r = client.get('/dataset_info/using_title', query_string={'title': title})
    end = timer()
    print()
    print('elapsed: ', end - start)
    response = json.loads(r.data)
    assert 'result' in response
    assert len(response['result']) == 1
    result = response['result'][0]
    # print(result)
    assert "Generic mouse colon scaffold" == result['name']
    print_search_result(result)

    assert SCAFFOLD_FILE in result.keys()
    assert THUMBNAIL_IMAGE in result.keys()

def test_physiological_data_visualisation_search(client):
    #title = "Test case for physiological data visualisation"
    r = client.get('/dataset_info/using_pennsieve_identifier', query_string={'identifier': 141})
    response = json.loads(r.data)
    assert 'result' in response
    assert len(response['result']) == 1
    result = response['result'][0]
    # print(result)
    assert "Test case for physiological data visualisation" == result['name']
    assert PLOT_FILE in result.keys()
    #'text/vnd.abi.plot+tab-separated-values' and #'text/vnd.abi.plot+csv'
    assert len(result[PLOT_FILE]) == 21

def test_complex_title_dataset_search(client):
    title = "Spatial distribution and morphometric characterization of vagal efferents associated with the myenteric plexus of the rat stomach"

    start = timer()
    r = client.get('/dataset_info/using_title', query_string={'title': title})
    end = timer()
    print('elapsed: ', end - start)
    response = json.loads(r.data)
    assert 'result' in response
    assert len(response['result']) == 1
    result = response['result'][0]
    assert result['name'].startswith("Spatial distribution and morphometric characterization of vagal efferents ")

    assert COMMON_IMAGES in result.keys()


@pytest.mark.skip(reason="dois are not suitable for long term testing")
def test_doi_plot_annotation_dataset_search_version_1(client):
    # Test case for physiological data visualisation
    # This test is bound to fail at time due to the changing DOI.
    doi = "10.26275/duit-8aqu"
    start = timer()
    r = client.get('/dataset_info/using_doi', query_string={'doi': doi})
    end = timer()
    print()
    print('elapsed: ', end - start)
    response = json.loads(r.data)
    assert 'result' in response
    assert len(response['result']) == 0


@pytest.mark.skip(reason="dois are not suitable for long term testing")
def test_doi_plot_annotation_dataset_search_version_2(client):
    # Test case for physiological data visualisation
    # This test is bound to fail at time due to the changing DOI.
    # This DOI belongs to dataset 141
    doi = "10.26275/9qws-u3px"
    start = timer()
    r = client.get('/dataset_info/using_doi', query_string={'doi': doi})
    end = timer()
    print()
    print('elapsed: ', end - start)
    response = json.loads(r.data)
    assert 'result' in response
    assert len(response['result']) == 1
    result = response['result'][0]
    assert "Test case for physiological data visualisation" == result['name']
    # print_search_result(result)
    assert PLOT_FILE in result


def test_title_plot_annotation_dataset_search(client):
    print()
    title = "Test case for physiological data visualisation"
    # title = "High resolution manometry"
    start = timer()
    r = client.get('/dataset_info/using_title', query_string={'title': title})
    end = timer()
    print('elapsed: ', end - start)
    response = json.loads(r.data)
    assert 'result' in response
    assert len(response['result']) == 1
    result = response['result'][0]
    assert "Test case for physiological data visualisation" == result['name']

    assert PLOT_FILE in result
    assert len(result[PLOT_FILE])

    first_result = result[PLOT_FILE][0]
    assert 'datacite' in first_result
    assert 'supplemental_json_metadata' in first_result['datacite']
    assert 'isDescribedBy' in first_result['datacite']
    assert 'description' in first_result['datacite']['supplemental_json_metadata']
    plot_description = json.loads(first_result['datacite']['supplemental_json_metadata']['description'])
    assert 'version' in plot_description
    assert 'type' in plot_description
    assert 'attrs' in plot_description
    assert plot_description['type'] == 'plot'


def test_object_identifier_dataset_search(client):
    #Dataset 141
    print()
    identifier = "package:cc63e3f6-c2d6-4355-9c46-604660377bcb"
    start = timer()
    r = client.get('/dataset_info/using_object_identifier', query_string={'identifier': identifier})
    end = timer()
    print('elapsed: ', end - start)
    response = json.loads(r.data)
    assert 'result' in response
    assert len(response['result']) == 1
    result = response['result'][0]
    assert "Test case for physiological data visualisation" == result['name']

    assert PLOT_FILE in result
    assert len(result[PLOT_FILE])

    first_result = result[PLOT_FILE][0]
    assert 'datacite' in first_result
    assert 'supplemental_json_metadata' in first_result['datacite']
    assert 'isDescribedBy' in first_result['datacite']
    assert 'description' in first_result['datacite']['supplemental_json_metadata']
    plot_description = json.loads(first_result['datacite']['supplemental_json_metadata']['description'])
    assert 'version' in plot_description
    assert 'type' in plot_description
    assert 'attrs' in plot_description
    assert plot_description['type'] == 'plot'


def test_pennsieve_identifier_dataset_search(client):
    print()
    identifier = "43"
    start = timer()
    r = client.get('/dataset_info/using_pennsieve_identifier', query_string={'identifier': identifier})
    end = timer()
    print('elapsed: ', end - start)
    response = json.loads(r.data)
    assert 'result' in response
    assert len(response['result']) == 1
    result = response['result'][0]
    assert "human islet microvasculature analysis" == result['name'].lower()

    assert BIOLUCIDA_3D in result
    assert len(result[BIOLUCIDA_3D])

    assert 'readme' in result

    first_result = result[BIOLUCIDA_3D][0]
    print(len(result[BIOLUCIDA_3D]))
    print(first_result)
    for r in result[BIOLUCIDA_3D]:
        print(r['dataset']['path'])
