test_services = [
    {
        "key": "simcore/services/dynamic/sim4life",
        "title": "Sim4Life",
        "description": "This is the first service that has a very nice description. And it is also sim4life for the web",
        "view_url": "https://osparc.io/service/2",
        "thumbnail": "https://www.hopkinsmedicine.org/-/media/images/health/1_-conditions/brain/brain-anatomy-teaser.ashx",
        "file_extensions": ["smash", "h5"]
    },
    {
        "key": "simcore/services/dynamic/rawg",
        "title": "Service 2",
        "description": "This is the first service that has a very nice description",
        "view_url": "https://osparc.io/service/2",
        "thumbnail": "https://www.hopkinsmedicine.org/-/media/images/health/1_-conditions/brain/brain-anatomy-teaser.ashx",
        "file_extensions": ["csv"]
    },
    {
        "key": "simcore/services/dynamic/aservice",
        "title": "Service 3",
        "description": "This is the first service that has a very nice description",
        "view_url": "https://osparc.io/service/3",
        "thumbnail": "https://m.media-amazon.com/images/M/MV5BN2EyYTkwNjUtY2I5MS00ZTFmLWI2ZGUtODBhMDgzZjk1YjgwXkEyXkFqcGdeQXVyNDc2NjEyMw@@._V1_.jpg",
        "file_extensions": ["csv", "xlsx"]
    },
    {
        "key": "simcore/services/dynamic/sim2life",
        "title": "Service 4",
        "description": "This is the first service that has a very nice description crocodile",
        "view_url": "https://osparc.io/service/4",
        "thumbnail": "https://www.longrunexploration.com/upload/main_banner/2/05/banner.jpg",
        "file_extensions": ["smash", "jpg"]
    },
    {
        "key": "simcore/services/dynamic/bobo",
        "title": "Service 5",
        "description": "This is the first service that has a very nice description",
        "view_url": "https://osparc.io/service/5",
        "thumbnail": "https://www.hopkinsmedicine.org/-/media/images/health/1_-conditions/brain/brain-anatomy-teaser.ashx",
        "file_extensions": ["jpg", "tif"]
    },
    {
        "key": "simcore/services/dynamic/crocoservice",
        "title": "Service 7",
        "description": "This is the first service that has a very nice description",
        "view_url": "https://osparc.io/service/7",
        "thumbnail": "https://www.hopkinsmedicine.org/-/media/images/health/1_-conditions/brain/brain-anatomy-teaser.ashx",
        "file_extensions": ["croc"]
    },
    {
        "key": "simcore/services/dynamic/si",
        "title": "Service CrocodilE",
        "description": "This is the first service that has a very nice description",
        "view_url": "https://osparc.io/service/8",
        "thumbnail": "https://www.hopkinsmedicine.org/-/media/images/health/1_-conditions/brain/brain-anatomy-teaser.ashx",
        "file_extensions": ["*"]
    },
    {
        "key": "simcore/services/dynamic/no",
        "title": "Service 9",
        "description": "This is the first service that has a very nice description",
        "view_url": "https://osparc.io/service/9",
        "thumbnail": "https://www.hopkinsmedicine.org/-/media/images/health/1_-conditions/brain/brain-anatomy-teaser.ashx",
        "file_extensions": ["croc", "jpg", "png"]
    },
]

class OSparcServices:
    services = []
    file_extensions = {}


    def __init__(self, services=None):
        if services:
            self.services = services
        else:
            self.services = test_services
        self.generate_file_extensions()


    def search_services(self, search_terms):

        if isinstance(search_terms, str) and len(search_terms):

            norm_terms = search_terms.lower()

            def filter_fn(service):
                if norm_terms in service.get('title').lower():
                    return True
                if norm_terms in service.get('description').lower():
                    return True
                return False

            result = filter(
                filter_fn,
                self.services
            )

            return list(result)

        else:

            return self.services


    def generate_file_extensions(self):

        self.file_extensions = {}

        for service in self.services:    

            for extension in service.get('file_extensions'):

                if not self.file_extensions.get(extension):
                    self.file_extensions[extension] = []

                self.file_extensions[extension].append(service.get('key'))


    def set_services(self, services):
        self.services = services
        self.generate_file_extensions()