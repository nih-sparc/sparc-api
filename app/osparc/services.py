class OSparcServices:
    services = []
    file_extensions = {}


    def __init__(self, services=None):
        if services:
            self.services = services
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

            for extension in service.get('file_extensions', []):

                norm_extension = extension.lower()

                if not self.file_extensions.get(norm_extension):
                    self.file_extensions[norm_extension] = []

                self.file_extensions[norm_extension].append({
                    "title": service.get("title"),
                    "view_url": service.get("view_url")
                })


    def set_services(self, services):
        self.services = services
        self.generate_file_extensions()
