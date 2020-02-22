from template.controller.table import Table

class disk():
    def __init__(self, page_size):
        self.page_size = page_size

    def writePage(self, pages):
        self.page_record.update({pages.id:pages.pages[0].num_records})
        self.page_len = len(pages.pages)
        filename = str(pages.id)
        f = open(filename, 'wb')
        for i in range(len(pages.pages)):
            f.write(pages.pages[i].data)
        f.close()

    def readPage(self, pageID):
        filename = str(pageID)
        f = open(filename, 'rb')

        pages = []
        for i in range(self.page_len):
            page = Page()
            page.num_records = self.page_record[pageID]
            page.data = f.read()
            pages.append(page)
        ret_pages = Pages(pageID, pages)
        return ret_pages

    def deletePage(self, page):
        pass