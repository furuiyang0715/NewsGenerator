import datetime
import pprint
import traceback

from base import NewsBase

_today = datetime.datetime.combine(datetime.datetime.today(), datetime.time.min)


class OrganizationEvaluation(NewsBase):

    def __init__(self):
        super(OrganizationEvaluation, self).__init__()
        self.source_table = 'rrp_rpt_secu_rat'
        self.bg_client = None

    def __del__(self):
        if self.bg_client:
            self.bg_client.dispose()

    def get_pub_today(self, day=_today):
        """发布时间在指定日期的全部数据"""
        self.bg_client = self._init_pool(self.bigdata_cfg)
        sql = '''select com_id,trd_code,pub_dt from {} where pub_dt = '{}';'''.format(self.source_table, day)
        ret = self.bg_client.select_all(sql)
        return ret

    def check_pub_first(self, data):
        """判断是否是机构首次发布"""
        sql = '''select com_id,trd_code,pub_dt from {} where com_id = '{}' and trd_code = '{}' and pub_dt < '{}'; 
        '''.format(self.source_table, data.get("com_id"), data.get("trd_code"), data.get("pub_dt"))
        ret = self.bg_client.select_all(sql)
        if not ret:
            return True
        else:
            return False

    def start(self):
        datas = self.get_pub_today()
        for data in datas:
            is_first = self.check_pub_first(data)
            print(is_first)


if __name__ == "__main__":
    oe = OrganizationEvaluation()
    oe.start()
