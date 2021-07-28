import json
import pytest
from app import app

from timeit import default_timer as timer

from app.scicrunch_processing_common import SCAFFOLD_FILE, PLOT_FILE, COMMON_IMAGES, SCAFFOLD_THUMBNAIL, NAME


@pytest.fixture
def client():
    # Spin up test flask app
    app.config['TESTING'] = True
    return app.test_client()


def test_current_doi_list(client):
    doi_stem = "doi:"
    print("go")
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
        if stored_doi not in known_doi_s:
            changes.append(doi)

    if len(changes):
        print(f'The current DOI list has changed {len(changes)}.')
        print(changes)

    assert len(changes) == 0


known_doi_s = [
    "10.26275/0ag5-j3x7",
    "10.26275/0ce8-cuwi",
    "10.26275/0lfa-i4by",
    "10.26275/0y4e-eskx",
    "10.26275/1upo-xvkt",
    "10.26275/255m-00nj",
    "10.26275/3aqc-fkry",
    "10.26275/3m8n-0owa",
    "10.26275/4htn-7v5w",
    "10.26275/4qvr-kwzq",
    "10.26275/5jki-b4er",
    "10.26275/63lh-hdz5",
    "10.26275/6gqy-iwhm",
    "10.26275/6paz-gj9j",
    "10.26275/717v-zsi2",
    "10.26275/8pc2-rhu2",
    "10.26275/8vi1-tcsp",
    "10.26275/advv-1awo",
    "10.26275/ajkk-l7xd",
    "10.26275/bjp1-ppqo",
    "10.26275/boe7-1bms",
    "10.26275/bovi-ivq6",
    "10.26275/c4xq-9kl0",
    "10.26275/ckgb-5ewo",
    "10.26275/dap3-ckep",
    "10.26275/dbja-zabv",
    "10.26275/dn1d-owj9",
    "10.26275/duit-8aqu",
    "10.26275/duz8-mq3n",
    "10.26275/dwzu-xtmj",
    "10.26275/eyik-qjhm",
    "10.26275/fbzm-3eii",
    "10.26275/fcrd-lbid",
    "10.26275/gdot-t59p",
    "10.26275/ge74-ypxd",
    "10.26275/guqw-r3ca",
    "10.26275/h7dq-lsuy",
    "10.26275/hyhn-x3nw",
    "10.26275/iami-zirb",
    "10.26275/ibeu-njry",
    "10.26275/iiwv-k07f",
    "10.26275/ilb9-0e2a",
    "10.26275/ilkm-9f8r",
    "10.26275/iojl-pirh",
    "10.26275/jdws-d7md",
    "10.26275/jg3k-z5qm",
    "10.26275/jl5t-xfgu",
    "10.26275/jqej-3rao",
    "10.26275/kabb-mkvu",
    "10.26275/m9ti-0pbj",
    "10.26275/ma8j-sjuy",
    "10.26275/maq2-eii4",
    "10.26275/mvwc-fnqm",
    "10.26275/mzth-oxbk",
    "10.26275/nj9c-gqyg",
    "10.26275/nluu-1ews",
    "10.26275/nnyt-bqpg",
    "10.26275/nxfv-p3ol",
    "10.26275/nyah-5kq9",
    "10.26275/nyuq-anco",
    "10.26275/o9qr-l4x9",
    "10.26275/osy6-dn3o",
    "10.26275/owri-mpsx",
    "10.26275/pgr9-bk2e",
    "10.26275/pidf-15l3",
    "10.26275/pkgd-bopz",
    "10.26275/ppgj-qqpf",
    "10.26275/prjd-jhoc",
    "10.26275/pzek-91wx",
    "10.26275/qh3q-elj6",
    "10.26275/qkzi-b1mq",
    "10.26275/qmqb-uqlz",
    "10.26275/r3fm-edry",
    "10.26275/rets-qdch",
    "10.26275/rtzw-x9u4",
    "10.26275/s7ej-b72v",
    "10.26275/spfh-lx9g",
    "10.26275/sydt-lkiw",
    "10.26275/t4ng-2zm6",
    "10.26275/tuof-9odl",
    "10.26275/tv7g-o8ff",
    "10.26275/tyza-wdi3",
    "10.26275/u17s-hcn0",
    "10.26275/ukz3-0fao",
    "10.26275/uuao-pk37",
    "10.26275/uztw-z5sc",
    "10.26275/vm18-qapj",
    "10.26275/w027-cisv",
    "10.26275/w4my-puqm",
    "10.26275/wcje-hxib",
    "10.26275/wzry-sf7v",
    "10.26275/xkoa-oqec",
    "10.26275/xmsp-wwtu",
    "10.26275/xmyx-rnm9",
    "10.26275/yztm-kos4",
    "10.26275/z3ab-7j9y",
    "10.26275/zdxd-84xz",
    "10.26275/zxe9-o3ss",
]


def print_search_result(result):
    keys = result.keys()

    found = False
    messages = []
    if COMMON_IMAGES in keys:
        found = True
        messages.append(f" - Found common image: {len(result[COMMON_IMAGES])}")
    if 'large-2d-image' in keys:
        found = True
        messages.append(f" - Found large 2D image: {len(result['large-2d-image'])}")
    if 'large-3d-image' in keys:
        found = True
        messages.append(f" - Found large 3D image: {len(result['large-3d-image'])}")
    if SCAFFOLD_FILE in keys:
        found = True
        messages.append(f" - Found scaffold: {len(result[SCAFFOLD_FILE])}")
    if PLOT_FILE in keys:
        found = True
        messages.append(f" - Found plot: {len(result[PLOT_FILE])}")
    if SCAFFOLD_THUMBNAIL in keys:
        found = True
        messages.append(f" - Found scaffold thumbnail: {len(result[SCAFFOLD_THUMBNAIL])}")
    if 'mp4' in keys:
        found = True
        messages.append(f" - Found video: {len(result['mp4'])}")
    if 'mbf-segmentation' in keys:
        found = True
        messages.append(f" - Found segmentation: {len(result['mbf-segmentation'])}")

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


def test_all_known_doi_search(client):
    for index, doi in enumerate(known_doi_s):
        start = timer()
        print(f'Results for DOI [{index + 1}/{len(known_doi_s)}]: {doi}')
        r = client.get('/dataset_info/using_doi', query_string={'doi': doi})
        # r = client.get('/search/contributors')
        end = timer()
        print('elapsed: ', end - start)
        response = json.loads(r.data)
        assert 'result' in response
        if len(response['result']) == 1:
            assert len(response['result']) == 1
            result = response['result'][0]
        # print('DOI: ', doi)
        # print(result)
            print_search_result(result)
        elif len(response['result']) == 0:
            print(f'No result for {doi}!')
        else:
            print(f'Multiple results for {doi}!!')


def test_doi_dataset_search(client):
    doi = "10.26275/0ce8-cuwi"
    start = timer()
    r = client.get('/dataset_info/using_doi', query_string={'doi': doi})
    end = timer()
    print('elapsed: ', end - start)
    response = json.loads(r.data)
    assert 1 == len(response['result'])
    result = response['result'][0]
    print(f'took: {result["took"]}')
    print("Title :", result['name'])
    print_search_result(result)
    print_mime_paths(result['files'])

    assert 3 == 2


def test_doi_wcje_hxib_dataset_search(client):
    # Mapping of ICN Neurons in a 3D Reconstructed Rat Heart
    doi = "10.26275/wcje-hxib"
    start = timer()
    r = client.get('/dataset_info/using_doi', query_string={'doi': doi})
    # r = client.get('/search/contributors')
    end = timer()
    print('elapsed: ', end - start)
    response = json.loads(r.data)
    assert 'result' in response
    assert len(response['result']) == 1
    result = response['result'][0]
    assert "Mapping of ICN Neurons in a 3D Reconstructed Rat Heart" == result['item']['name']
    print(result)
    print_search_result(result)

    assert 'mbf-segmentation' in result


def test_doi_nxfv_p3ol_dataset_search(client):
    # Three mp4 videos
    doi = "10.26275/nxfv-p3ol"
    start = timer()
    r = client.get('/dataset_info/using_doi', query_string={'doi': doi})
    # r = client.get('/search/contributors')
    end = timer()
    print('elapsed: ', end - start)
    response = json.loads(r.data)
    assert 'result' in response
    assert len(response['result']) == 1
    result = response['result'][0]
    assert "CLARITY and three-dimensional (3D) imaging of the mouse and porcine colonic innervation" == result['item']['name']
    print_search_result(result)
    print_mime_paths(result['files'])

    assert 'mbf-segmentation' in ['keys']


def test_doi_0y4e_eskx_dataset_search(client):
    # Nine 2D Large images
    doi = "10.26275/0y4e-eskx"
    start = timer()
    r = client.get('/dataset_info/using_doi', query_string={'doi': doi})
    # r = client.get('/search/contributors')
    end = timer()
    print('elapsed: ', end - start)
    response = json.loads(r.data)
    assert 'result' in response
    assert len(response['result']) == 1
    result = response['result'][0]
    assert "Distribution of nitregic, cholinergic and all MP neurons along the colon using nNOS-GCaMP3 mice, ChAT-GCaMP3 and Wnt1-GCaMP3 mice" == result['item']['name']
    print_search_result(result)
    print_mime_paths(result['files'])

    assert 'mbf-segmentation' in ['keys']


def test_doi_qh3q_elj6_dataset_search(client):
    # Nine 2D Large images
    doi = "10.26275/qh3q-elj6"
    start = timer()
    r = client.get('/dataset_info/using_doi', query_string={'doi': doi})
    # r = client.get('/search/contributors')
    end = timer()
    print('elapsed: ', end - start)
    response = json.loads(r.data)
    assert 'result' in response
    assert len(response['result']) == 1
    result = response['result'][0]
    assert "Influence of left vagal stimulus pulse parameters on vagal and gastric activity in rat" == result['item']['name']
    print_search_result(result)
    print_mime_paths(result['files'])

    assert 'mbf-segmentation' in ['keys']


def test_generic_mouse_colon_dataset_search(client):
    title = "Generic mouse colon scaffold"
    start = timer()
    r = client.get('/dataset_info/using_title', query_string={'title': title})
    end = timer()
    print('elapsed: ', end - start)
    response = json.loads(r.data)
    assert 'result' in response
    assert len(response['result']) == 1
    result = response['result'][0]
    print(result)
    assert "Generic mouse colon scaffold" == result['name']
    print_search_result(result)

    assert SCAFFOLD_FILE in result.keys()
    assert SCAFFOLD_THUMBNAIL in result.keys()


def test_complex_title_dataset_search(client):
    title = "Spatial distribution and morphometric characterization of vagal afferents (specifically: intraganglionic laminar endings (IGLEs)) associated with the myenteric " \
            "plexus of the rat stomach "
    start = timer()
    r = client.get('/dataset_info/using_title', query_string={'title': title})
    end = timer()
    print('elapsed: ', end - start)
    response = json.loads(r.data)
    assert 'result' in response
    assert len(response['result']) == 1
    result = response['result'][0]
    assert result['name'].startswith("Spatial distribution and morphometric characterization of vagal afferents (specifically: ")

    assert COMMON_IMAGES in result.keys()


def test_doi_plot_annotation_dataset_search_version_1(client):
    # Test case for physiological data visualisation
    # This test is bound to fail at time due to the changing DOI.
    doi = "10.26275/duit-8aqu"
    start = timer()
    r = client.get('/dataset_info/using_doi', query_string={'doi': doi})
    end = timer()
    print('elapsed: ', end - start)
    response = json.loads(r.data)
    assert 'result' in response
    assert len(response['result']) == 1
    result = response['result'][0]
    assert "Test case for physiological data visualisation" == result['name']
    print_search_result(result)
    print(result)

    assert PLOT_FILE in result


def test_doi_plot_annotation_dataset_search_version_2(client):
    # Test case for physiological data visualisation
    # This test is bound to fail at time due to the changing DOI.
    doi = "10.26275/4i5w-w7ai"
    start = timer()
    r = client.get('/dataset_info/using_doi', query_string={'doi': doi})
    end = timer()
    print('elapsed: ', end - start)
    response = json.loads(r.data)
    assert 'result' in response
    assert len(response['result']) == 1
    result = response['result'][0]
    assert "Test case for physiological data visualisation" == result['name']
    print_search_result(result)
    print(result)

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
