from base import NewsBase


class OrganizationEvaluation(NewsBase):

    def __init__(self):
        super(OrganizationEvaluation, self).__init__()
        self.source_table = 'rrp_rpt_secu_rat'

    def start(self):
        # 获取发布时间等于今天的
        client = self._init_pool(self.bigdata_cfg)
        sql = '''select * from {} limit 1; '''.format(self.source_table)
        ret = client.select_one(sql)
        print(ret)


if __name__ == "__main__":
    oe = OrganizationEvaluation()
    oe.start()
