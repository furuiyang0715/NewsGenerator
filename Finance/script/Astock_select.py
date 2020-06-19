from base import NewsBase


class Selector(NewsBase):
    def __init__(self):
        super(Selector, self).__init__()
        self.table_name = 'news_generate_finance'

    def remove_records_nota(self):
        self._target_init()
        Acompany_codes = tuple(self.total_company_codes().keys())
        sql = 'delete from {} where  CompanyCode not in {}; '.format(self.table_name, Acompany_codes)
        count = self.target_client.delete(sql)
        print(count)


if __name__ == "__main__":
    selector = Selector()
    selector.remove_records_nota()