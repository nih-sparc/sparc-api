services = [
    {
        "id": 1,
        "title": "Service 1",
        "description": "This is the first service that has a very nice description",
        "linkURL": "https://osparc.io/service/1",
        "thumbnail": "https://www.hopkinsmedicine.org/-/media/images/health/1_-conditions/brain/brain-anatomy-teaser.ashx"
    },
    {
        "id": 12,
        "title": "Service 2",
        "description": "This is the first service that has a very nice description",
        "linkURL": "https://osparc.io/service/2",
        "thumbnail": "https://www.hopkinsmedicine.org/-/media/images/health/1_-conditions/brain/brain-anatomy-teaser.ashx"
    },
    {
        "id": 13,
        "title": "Service 3",
        "description": "This is the first service that has a very nice description",
        "linkURL": "https://osparc.io/service/3",
        "thumbnail": "https://www.hopkinsmedicine.org/-/media/images/health/1_-conditions/brain/brain-anatomy-teaser.ashx"
    },
    {
        "id": 14,
        "title": "Service 4",
        "description": "This is the first service that has a very nice description crocodile",
        "linkURL": "https://osparc.io/service/4",
        "thumbnail": "https://www.hopkinsmedicine.org/-/media/images/health/1_-conditions/brain/brain-anatomy-teaser.ashx"
    },
    {
        "id": 15,
        "title": "Service 5",
        "description": "This is the first service that has a very nice description",
        "linkURL": "https://osparc.io/service/5",
        "thumbnail": "https://www.hopkinsmedicine.org/-/media/images/health/1_-conditions/brain/brain-anatomy-teaser.ashx"
    },
    {
        "id": 16,
        "title": "Service 7",
        "description": "This is the first service that has a very nice description",
        "linkURL": "https://osparc.io/service/7",
        "thumbnail": "https://www.hopkinsmedicine.org/-/media/images/health/1_-conditions/brain/brain-anatomy-teaser.ashx"
    },
    {
        "id": 17,
        "title": "Service CrocodilE",
        "description": "This is the first service that has a very nice description",
        "linkURL": "https://osparc.io/service/8",
        "thumbnail": "https://www.hopkinsmedicine.org/-/media/images/health/1_-conditions/brain/brain-anatomy-teaser.ashx"
    },
    {
        "id": 18,
        "title": "Service 9",
        "description": "This is the first service that has a very nice description",
        "linkURL": "https://osparc.io/service/9",
        "thumbnail": "https://www.hopkinsmedicine.org/-/media/images/health/1_-conditions/brain/brain-anatomy-teaser.ashx"
    },
]


def search_services(search_terms):

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
            services
        )

        return list(result)

    else:

        return services