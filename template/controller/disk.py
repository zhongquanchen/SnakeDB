from template.controller.table import Table


class disk:
    def __init__(self, page_size):
        self.page_size = page_size

    def write_page(self, page):
        pass

    def allocate_page(self):
        pass

    def delete_page(self, page):
        pass
