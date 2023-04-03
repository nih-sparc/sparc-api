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
    'inode/vnd.abi.scaffold+directory': SCAFFOLD_DIR,
    'inode/vnd.abi.scaffold+file': SCAFFOLD_FILE,
    'inode/vnd.abi.scaffold+thumbnail': THUMBNAIL_IMAGE,
    'inode/vnd.abi.scaffold.thumbnail+file': THUMBNAIL_IMAGE,
    'inode/vnd.abi.scaffold.view+file': SCAFFOLD_VIEW_FILE,
    'text/vnd.abi.plot+tab-separated-values': PLOT_FILE,
    'text/vnd.abi.plot+csv': PLOT_FILE,
    'image/png': COMMON_IMAGES,
    'image/tiff': 'tiff-image',
    'image/tif': 'tiff-image',
    'image/jpeg': COMMON_IMAGES,
    'image/jpx': BIOLUCIDA_3D,
    'image/vnd.ome.xml+jpx': BIOLUCIDA_3D,
    'image/jp2': BIOLUCIDA_2D,
    'image/vnd.ome.xml+jp2': BIOLUCIDA_2D,
    'video/mp4': VIDEO
}

SKIPPED_MIME_TYPES = [
    'application/fastq',
    'application/json',
    'application/octet-stream',
    'application/pdf',
    'application/rar',
    'application/rdf+xml',
    'application/vnd.cambridge-electronic-designced.spike2.32.data',
    'application/vnd.cambridge-electronic-designced.spike2.64.data',
    'application/vnd.cambridge-electronic-designced.spike2.resource+xml',
    'application/vnd.ms-excel',
    'application/vnd.oasis.opendocument.database',
    'application/vnd.openxmlformats-officedocument.presentationml.presentation',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    'application/vnd.wolfram.mathematica.package',
    'application/x-bzip-compressed-fastq',
    'application/x-gz-compressed-fastq',
    'application/x-matlab',
    'application/x-matlab-data',
    'application/x-msdos-program',
    'application/x-tar',
    'application/xml',
    'application/zip',
    'audio/midi',
    'audio/x-mod',
    'chemical/x-gamess-input',
    'chemical/x-ncbi-asn1-ascii',
    'font/otf',
    'font/ttf',
    'image/gznii',
    'image/vnd.nikon.nd2',
    'image/vnd.ome.xml+jp2',
    'image/vnd.ome.xml+jpx',
    'image/vnd.ome.xml+tiff',
    'image/vnd.zeiss.czi',
    'image/x-coreldraw',
    'inode/directory',
    'inode/vnd.abi.plot+thumbnail',
    'model/obj',
    'text/Tab-separated-values',
    'text/css',
    'text/h323',
    'text/html',
    'text/markdown',
    'text/plain',
    'text/tab-separated-values',
    'text/x-c++src',
    'text/x-chdr',
    'text/x-python',
    'text/x-sh',
    'video/x-msvideo',
]

SKIPPED_OBJ_ATTRIBUTES = [
    'bytes',
    'checksums',
    'distributions',
    'issupplementalto',
    'updated'
]


def map_mime_type(mime_type, obj):
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
