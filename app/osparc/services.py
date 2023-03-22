services = [
    {
        "name_key": "simcore/services/dynamic/sim4life",
        "name": "Sim4Life",
        "description": "This is the first service that has a very nice description. And it is also sim4life for the web",
        "view_url": "https://osparc.io/service/2",
        "thumbnail": "https://www.hopkinsmedicine.org/-/media/images/health/1_-conditions/brain/brain-anatomy-teaser.ashx",
        "file_extensions": ["smash", "h5"]
    },
    {
        "name_key": "simcore/services/dynamic/rawg",
        "name": "Service 2",
        "description": "This is the first service that has a very nice description",
        "view_url": "https://osparc.io/service/2",
        "thumbnail": "https://www.hopkinsmedicine.org/-/media/images/health/1_-conditions/brain/brain-anatomy-teaser.ashx",
        "file_extensions": ["csv"]
    },
    {
        "name_key": "simcore/services/dynamic/aservice",
        "name": "Service 3",
        "description": "This is the first service that has a very nice description",
        "view_url": "https://osparc.io/service/3",
        "thumbnail": "https://m.media-amazon.com/images/M/MV5BN2EyYTkwNjUtY2I5MS00ZTFmLWI2ZGUtODBhMDgzZjk1YjgwXkEyXkFqcGdeQXVyNDc2NjEyMw@@._V1_.jpg",
        "file_extensions": ["csv", "xlsx"]
    },
    {
        "name_key": "simcore/services/dynamic/sim2life",
        "name": "Service 4",
        "description": "This is the first service that has a very nice description crocodile",
        "view_url": "https://osparc.io/service/4",
        "thumbnail": "https://www.longrunexploration.com/upload/main_banner/2/05/banner.jpg",
        "file_extensions": ["smash", "jpg"]
    },
    {
        "name_key": "simcore/services/dynamic/bobo",
        "name": "Service 5",
        "description": "This is the first service that has a very nice description",
        "view_url": "https://osparc.io/service/5",
        "thumbnail": "https://www.hopkinsmedicine.org/-/media/images/health/1_-conditions/brain/brain-anatomy-teaser.ashx",
        "file_extensions": ["jpg", "tif"]
    },
    {
        "name_key": "simcore/services/dynamic/crocoservice",
        "name": "Service 7",
        "description": "This is the first service that has a very nice description",
        "view_url": "https://osparc.io/service/7",
        "thumbnail": "https://www.hopkinsmedicine.org/-/media/images/health/1_-conditions/brain/brain-anatomy-teaser.ashx",
        "file_extensions": ["croc"]
    },
    {
        "name_key": "simcore/services/dynamic/si",
        "name": "Service CrocodilE",
        "description": "This is the first service that has a very nice description",
        "view_url": "https://osparc.io/service/8",
        "thumbnail": "https://www.hopkinsmedicine.org/-/media/images/health/1_-conditions/brain/brain-anatomy-teaser.ashx",
        "file_extensions": ["*"]
    },
    {
        "name_key": "simcore/services/dynamic/no",
        "name": "Service 9",
        "description": "This is the first service that has a very nice description",
        "view_url": "https://osparc.io/service/9",
        "thumbnail": "https://www.hopkinsmedicine.org/-/media/images/health/1_-conditions/brain/brain-anatomy-teaser.ashx",
        "file_extensions": ["croc", "jpg", "png"]
    },
]

file_extensions = None


def search_services(search_terms):

    if isinstance(search_terms, str) and len(search_terms):

        norm_terms = search_terms.lower()

        def filter_fn(service):
            if norm_terms in service.get('name').lower():
                return True
            if norm_terms in service.get('description').lower():
                return True
            return False

        result = filter(
            filter_fn,
            services
        )

        return list(result)

    else:

        return services


def generate_file_extensions():

    global file_extensions

    if file_extensions is None:

        file_extensions = {}

        for service in services:    

            for extension in service.get('file_extensions'):

                if not file_extensions.get(extension):

                    file_extensions[extension] = []

                file_extensions[extension].append(service.get('name_key'))

    return file_extensions