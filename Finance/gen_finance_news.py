from Finance.base import NewsBase


class GenFiance(NewsBase):
    def __init__(self, company_code):
        super(GenFiance, self).__init__()
        self.company_code = company_code
