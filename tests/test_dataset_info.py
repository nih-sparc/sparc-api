import json
import pytest
from app import app

from timeit import default_timer as timer


@pytest.fixture
def client():
    # Spin up test flask app
    app.config['TESTING'] = True
    return app.test_client()


def no_test_all_dataset_content(client):
    print("go")
    start = timer()
    r = client.get('/search/', query_string={'query': 'contributors', 'limit': 40})
    # r = client.get('/search/contributors')
    end = timer()
    print("done", end - start)
    response = json.loads(r.data)
    assert r.status_code == 200
    assert 'numberOfHits' in response.keys()

    print(len(response))
    print(type(response))
    print(response.keys())
    print(response['numberOfHits'])
    print(type(response['results']))
    results = response['results']
    print(len(results))
    # print(results[0])
    result_0 = results[0]
    print(type(result_0))
    print(result_0.keys())
    print(result_0['doi'])
    assert 3 == 2


def no_test_all_dataset_content(client):
    print("go")
    start = timer()
    r = client.get('/search/', query_string={'query': 'contributors'})
    # r = client.get('/search/contributors')
    end = timer()
    print("elapsed:", end - start)
    response = json.loads(r.data)
    page_index = 0
    page_size = 10
    number_of_hits = response['numberOfHits']
    while (page_index * page_size) < number_of_hits:
        response = json.loads(r.data)
        results = response['results']
        for result in results:
            print('doi:', result['doi'])

        page_index += 1
        start = timer()
        r = client.get('/search/', query_string={'query': 'contributors', 'start': (page_index * page_size)})
        end = timer()
        print("elapsed:", end - start)

    assert 3 == 2


doi_s = [
    "10.26275/h7dq-lsuy",
    "10.26275/4qvr-kwzq",
    "10.26275/0lfa-i4by",
    "10.26275/uup7-b1zb",
    "10.26275/r3fm-edry",
    "10.26275/tv7g-o8ff",
    "10.26275/uuao-pk37",
    "10.26275/tyza-wdi3",
    "10.26275/dbja-zabv",
    "10.26275/6paz-gj9j",
    "10.26275/iiwv-k07f",
    "10.26275/nnyt-bqpg",
    "10.26275/guqw-r3ca",
    "10.26275/zdxd-84xz",
    "10.26275/c4xq-9kl0",
    "10.26275/pzek-91wx",
    "10.26275/gdot-t59p",
    "10.26275/zxe9-o3ss",
    "10.26275/mzth-oxbk",
    "10.26275/vm18-qapj",
    "10.26275/byip-vbts",
    "10.26275/m9ti-0pbj",
    "10.26275/xmyx-rnm9",
    "10.26275/w4my-puqm",
    "10.26275/o9qr-l4x9",
    "10.26275/8pc2-rhu2",
    "10.26275/mvwc-fnqm",
    "10.26275/ppgj-qqpf",
    "10.26275/z3ab-7j9y",
    "10.26275/fcrd-lbid",
    "10.26275/boe7-1bms",
    "10.26275/maq2-eii4",
    "10.26275/ilb9-0e2a",
    "10.26275/t4ng-2zm6",
    "10.26275/spfh-lx9g",
    "10.26275/3m8n-0owa",
    "10.26275/prjd-jhoc",
    "10.26275/1upo-xvkt",
    "10.26275/5jki-b4er",
    "10.26275/pidf-15l3",
    "10.26275/ztgw-jz3r",
    "10.26275/tuof-9odl",
    "10.26275/bovi-ivq6",
    "10.26275/kabb-mkvu",
    "10.26275/xkoa-oqec",
    "10.26275/jdws-d7md",
    "10.26275/3aqc-fkry",
    "10.26275/jqej-3rao",
    "10.26275/duz8-mq3n",
    "10.26275/nyuq-anco",
    "10.26275/0ag5-j3x7",
    "10.26275/ibeu-njry",
    "10.26275/uztw-z5sc",
    "10.26275/yztm-kos4",
    "10.26275/hyhn-x3nw",
    "10.26275/eyik-qjhm",
    "10.26275/ukz3-0fao",
    "10.26275/dwzu-xtmj",
    "10.26275/63lh-hdz5",
    "10.26275/jl5t-xfgu",
    "10.26275/wzry-sf7v",
    "10.26275/owri-mpsx",
    "10.26275/nj9c-gqyg",
    "10.26275/osy6-dn3o",
    "10.26275/bjp1-ppqo",
    "10.26275/6gqy-iwhm",
    "10.26275/ajkk-l7xd",
    "10.26275/717v-zsi2",
    "10.26275/ge74-ypxd",
    "10.26275/rtzw-x9u4",
    "10.26275/iojl-pirh",
    "10.26275/xmsp-wwtu",
    "10.26275/sydt-lkiw",
    "10.26275/dap3-ckep",
    "10.26275/qh3q-elj6",
    "10.26275/w027-cisv",
    "10.26275/wcje-hxib",
    "10.26275/jg3k-z5qm",
    "10.26275/ilkm-9f8r",
    "10.26275/nluu-1ews",
    "10.26275/0ce8-cuwi",
    "10.26275/0y4e-eskx",
    "10.26275/fbzm-3eii",
    "10.26275/nyah-5kq9",
    "10.26275/pgr9-bk2e",
    "10.26275/qkzi-b1mq",
    "10.26275/nxfv-p3ol",
    "10.26275/255m-00nj",
]


def print_search_result(result):
    keys = result.keys()

    if 'generic-image' in keys:
        print("Found generic image: ", len(result['generic-image']))
    if 'large-2d-image' in keys:
        print("Found large 2D image: ", len(result['large-2d-image']))
    if 'abi-scaffold-file' in keys:
        print("Found scaffold: ", len(result['abi-scaffold-file']))
    if 'abi-scaffold-thumbnail' in keys:
        print("Found scaffold: ", len(result['abi-scaffold-thumbnail']))
    if 'mp4' in keys:
        print("Found video: ", len(result['mp4']))
    if 'mbf-segmentation' in keys:
        print("Found segmentation: ", len(result['mbf-segmentation']))


def print_mime_paths(files):
    for file_ in files:
        mimetype = file_.get('mimetype', 'not-specified')
        if mimetype == "inode/directory":
            pass
        elif mimetype == 'not-specified':
            pass
        else:
            print(file_['mimetype'], file_['dataset']['path'])


def no_test_doi_dataset_search(client):
    print("go")
    doi = "10.26275/uup7-b1zb"
    start = timer()
    r = client.get('/dataset_info_from_doi/', query_string={'doi': doi})
    # r = client.get('/search/contributors')
    end = timer()
    print('elapsed: ', end - start)
    response = json.loads(r.data)
    print(doi)
    assert 1 == response['numberOfHits']
    result = response['results'][0]
    print("Title :", result['name'])
    print_search_result(result)
    print_mime_paths(result['files'])

    assert 3 == 2


def no_test_all_doi_dataset_search(client):
    print("go")
    for doi in doi_s:
        start = timer()
        r = client.get('/dataset_info_from_doi/', query_string={'doi': doi})
        # r = client.get('/search/contributors')
        end = timer()
        print('elapsed: ', end - start)
        response = json.loads(r.data)
        assert 'result' in response
        assert len(response['result']) == 1
        result = response['result'][0]
        print('DOI: ', doi)
        print_search_result(result)

    assert 3 == 2


def test_doi_wcje_hxib_dataset_search(client):
    # Mapping of ICN Neurons in a 3D Reconstructed Rat Heart
    doi = "10.26275/wcje-hxib"
    start = timer()
    r = client.get('/dataset_info_from_doi/', query_string={'doi': doi})
    # r = client.get('/search/contributors')
    end = timer()
    print('elapsed: ', end - start)
    response = json.loads(r.data)
    assert 'result' in response
    assert len(response['result']) == 1
    result = response['result'][0]
    # assert "Mapping of ICN Neurons in a 3D Reconstructed Rat Heart" == result['name']
    print(result)
    print_search_result(result)

    assert 'mbf-segmentation' in result


def no_test_doi_nxfv_p3ol_dataset_search(client):
    # Three mp4 videos
    doi = "10.26275/nxfv-p3ol"
    start = timer()
    r = client.get('/dataset_info_from_doi/', query_string={'doi': doi})
    # r = client.get('/search/contributors')
    end = timer()
    print('elapsed: ', end - start)
    response = json.loads(r.data)
    assert 'result' in response
    assert len(response['result']) == 1
    result = response['result'][0]
    assert "CLARITY and three-dimensional (3D) imaging of the mouse and porcine colonic innervation" == result['name']
    print_search_result(result)
    print_mime_paths(result['files'])

    assert 'mbf-segmentation' in ['keys']


def no_test_doi_0y4e_eskx_dataset_search(client):
    # Nine 2D Large images
    doi = "10.26275/0y4e-eskx"
    start = timer()
    r = client.get('/dataset_info_from_doi/', query_string={'doi': doi})
    # r = client.get('/search/contributors')
    end = timer()
    print('elapsed: ', end - start)
    response = json.loads(r.data)
    assert 'result' in response
    assert len(response['result']) == 1
    result = response['result'][0]
    assert "Distribution of nitregic, cholinergic and all MP neurons along the colon using nNOS-GCaMP3 mice, ChAT-GCaMP3 and Wnt1-GCaMP3 mice" == result['name']
    print_search_result(result)
    print_mime_paths(result['files'])

    assert 'mbf-segmentation' in ['keys']


def no_test_doi_qh3q_elj6_dataset_search(client):
    # Nine 2D Large images
    doi = "10.26275/qh3q-elj6"
    start = timer()
    r = client.get('/dataset_info_from_doi/', query_string={'doi': doi})
    # r = client.get('/search/contributors')
    end = timer()
    print('elapsed: ', end - start)
    response = json.loads(r.data)
    assert 'result' in response
    assert len(response['result']) == 1
    result = response['result'][0]
    assert "Influence of left vagal stimulus pulse parameters on vagal and gastric activity in rat" == result['name']
    print_search_result(result)
    print_mime_paths(result['files'])

    assert 'mbf-segmentation' in ['keys']


def no_test_doi_4i5w_w7ai_dataset_search(client):
    # Test plot annotation dataset.
    doi = "10.26275/4i5w-w7ai"
    start = timer()
    r = client.get('/dataset_info_from_doi/', query_string={'doi': doi})
    # r = client.get('/search/contributors')
    end = timer()
    print('elapsed: ', end - start)
    response = json.loads(r.data)
    assert 'result' in response
    assert len(response['result']) == 1
    result = response['result'][0]
    print(result)
    assert "Influence of left vagal stimulus pulse parameters on vagal and gastric activity in rat" == result['name']
    print_search_result(result)
    print_mime_paths(result['files'])

    print(result.keys())

    assert 'mbf-segmentation' in result.keys()


def no_test_doi_eyik_qjhm_dataset_search(client):
    # Generic colon scaffold.
    doi = "10.26275/eyik-qjhm"
    start = timer()
    r = client.get('/dataset_info_from_doi/', query_string={'doi': doi})
    # r = client.get('/search/contributors')
    end = timer()
    print('elapsed: ', end - start)
    response = json.loads(r.data)
    assert 'result' in response
    assert len(response['result']) == 1
    result = response['result'][0]
    print(result)
    assert "Generic mouse colon scaffold" == result['name']
    print_search_result(result)
    print_mime_paths(result['files'])
    print(result.keys())
    print(result['scaffolds'])

    assert 'abi-scaffold-file' in result.keys()


def test_doi_maq2_eii4_dataset_search(client):
    # Quantified Morphology of the Pig Vagus Nerve
    doi = "10.26275/maq2-eii4"
    start = timer()
    r = client.get('/dataset_info_from_doi/', query_string={'doi': doi})
    # r = client.get('/search/contributors')
    end = timer()
    print('elapsed: ', end - start)
    response = json.loads(r.data)
    assert 'result' in response
    assert len(response['result']) == 1
    result = response['result'][0]
    # assert "Mapping of ICN Neurons in a 3D Reconstructed Rat Heart" == result['name']
    print(result)
    print_search_result(result)

    assert 'mbf-segmentation2' in result
