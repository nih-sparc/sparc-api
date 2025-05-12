from app.scicrunch_processing_skipped_mimetypes import SKIPPED_MIME_TYPES

NOT_SPECIFIED = 'not-specified'
SKIP = 'skip'

ADDITIONAL_LINKS = 'additionalLinks'
BIOLUCIDA_2D = 'biolucida-2d'
BIOLUCIDA_3D = 'biolucida-3d'
COMMON_IMAGES = 'common-images'
CONTEXT_FILE = 'abi-context-file'
CSV = 'csv'
NAME = 'name'
ORGANS = 'organs'
PLOT_FILE = 'abi-plot'
SEGMENTATION_FILES = 'mbf-segmentation'
SCAFFOLD_DIR = 'abi-scaffold-dir'
SCAFFOLD_FILE = 'abi-scaffold-metadata-file'
THUMBNAIL_IMAGE = 'abi-thumbnail'
SCAFFOLD_VIEW_FILE = 'abi-scaffold-view-file'
SIMULATION_FILE = 'abi-simulation-file'
FE_MODEL = 'fe-model'
VIDEO = 'video'
VERSION = 'version'
README = 'readme'
TITLE = 'title'


PASS_THROUGH_KEYS = [ADDITIONAL_LINKS, BIOLUCIDA_2D, BIOLUCIDA_3D, COMMON_IMAGES, CONTEXT_FILE, CSV, NAME, ORGANS, PLOT_FILE, README,
                     SEGMENTATION_FILES, SCAFFOLD_FILE, THUMBNAIL_IMAGE, SCAFFOLD_VIEW_FILE, SIMULATION_FILE, TITLE, VERSION, VIDEO]

MAPPED_MIME_TYPES = {
    'text/csv': CSV,
    'application/vnd.mbfbioscience.metadata+xml': SEGMENTATION_FILES,
    'application/vnd.mbfbioscience.neurolucida+xml': SEGMENTATION_FILES,
    'application/x.vnd.abi.context-information+json': CONTEXT_FILE,
    'application/x.vnd.abi.scaffold.meta+json': SCAFFOLD_FILE,
    'application/x.vnd.abi.scaffold.view+json': SCAFFOLD_VIEW_FILE,
    'application/x.vnd.abi.simulation+json': SIMULATION_FILE,
    'image/x.vnd.abi.thumbnail+jpeg': THUMBNAIL_IMAGE,
    'image/x.vnd.abi.thumbnail+png': THUMBNAIL_IMAGE,
    # 'text/vnd.abi.plot+thumbnail': THUMBNAIL_IMAGE, <-- Old incorrect annotation, needs to be corrected.
    'inode/vnd.abi.scaffold+directory': SCAFFOLD_DIR,
    'inode/vnd.abi.scaffold+file': SCAFFOLD_FILE,
    'inode/vnd.abi.scaffold+thumbnail': THUMBNAIL_IMAGE,
    'inode/vnd.abi.scaffold.thumbnail+file': THUMBNAIL_IMAGE,
    'inode/vnd.abi.scaffold.view+file': SCAFFOLD_VIEW_FILE,
    # 'inode/vnd.abi.plot+thumbnail': THUMBNAIL_IMAGE, <-- Old incorrect annotation, needs to be corrected.
    'text/vnd.abi.plot+tab-separated-values': PLOT_FILE,
    'text/x.vnd.abi.plot+tab-separated-values': PLOT_FILE,
    'text/vnd.abi.plot+csv': PLOT_FILE,
    'text/x.vnd.abi.plot+csv': PLOT_FILE,
    'image/png': COMMON_IMAGES,
    'image/tiff': 'tiff-image',
    'image/tif': 'tiff-image',
    'image/jpeg': COMMON_IMAGES,
    'image/jpx': BIOLUCIDA_3D,
    'image/vnd.ome.xml+jpx': BIOLUCIDA_3D,
    'image/jp2': BIOLUCIDA_2D,
    'image/vnd.ome.xml+jp2': BIOLUCIDA_2D,
    'model/stl': FE_MODEL,
    'model/obj': FE_MODEL,
    'video/mp4': VIDEO
}

SKIPPED_OBJ_ATTRIBUTES = [
    'bytes',
    'checksums',
    'distributions',
    'issupplementalto',
    'updated'
]


def map_mime_type(mime_type, obj):
    mime_type = mime_type.strip()

    if mime_type == '':
        return SKIP

    if mime_type == NOT_SPECIFIED:
        return SKIP

    lower_mime_type = mime_type.lower()

    if lower_mime_type in SKIPPED_MIME_TYPES:
        return SKIP

    if lower_mime_type in MAPPED_MIME_TYPES:
        if lower_mime_type in ["image/jpeg", "image/png"]:
            try:
                if obj['dataset']['path'].startswith('derivative'):
                    return SKIP
            except KeyError:
                return SKIP
        return MAPPED_MIME_TYPES[lower_mime_type]

    return NOT_SPECIFIED
