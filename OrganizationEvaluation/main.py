import datetime
import pprint
import traceback

from base import NewsBase

_today = datetime.datetime.combine(datetime.datetime.today(), datetime.time.min)


class OrganizationEvaluation(NewsBase):

    def __init__(self, day=_today):
        super(OrganizationEvaluation, self).__init__()
        self.source_table = 'rrp_rpt_secu_rat'
        self.idx_table = 'stk_quot_idx'
        self.day = day
        self.bg_client = None
        self.dc_client = None
        self.title_format = "{}今日获机构首次评级-{}"
        self.content_format = '{}月{}日{}（{}）获得{}首次评级 - {}，最新收盘价{}，涨幅{}%。'

    def __del__(self):
        if self.bg_client:
            self.bg_client.dispose()
        if self.dc_client:
            self.dc_client.dispose()

    def _bg_init(self):
        self.bg_client = self._init_pool(self.bigdata_cfg)

    def _dc_init(self):
        self.dc_client = self._init_pool(self.dc_cfg)

    def get_pub_today(self):
        """发布时间在指定日期的全部数据"""
        # trd_code  # 证券代码
        # secu_sht  # 证券简称
        # pub_dt # 发布日期
        # com_id   # 撰写机构编码
        # com_name # 撰写机构名称
        # rat_code  # 投资评级代码
        # rat_desc # 投资评级描述

        self._bg_init()
        select_fields = 'pub_dt,trd_code,secu_sht, com_id,com_name,rat_code,rat_desc'
        sql = '''select {} from {} where pub_dt = '{}';'''.format(select_fields, self.source_table, self.day)
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

    def get_item(self, data):
        trd_code = data.get("trd_code")
        secu_sht = data.get("secu_sht")
        com_id = data.get("com_id")
        com_name = data.get("com_name")
        rat_code = data.get("rat_code")
        rat_desc = data.get("rat_desc")

        item = dict()
        item["SecuCode"] = trd_code
        item['SecuAbbr'] = secu_sht
        inner_code = self.get_inner_code_bysecu(trd_code)
        item['InnerCode'] = inner_code
        item['ComId'] = com_id
        item['ComName'] = com_name
        item['RatCode'] = rat_code
        item['RatDesc'] = rat_desc

        # 杰瑞股份今日获机构首次评级-买入
        # "{}今日获机构首次评级-{}"
        title = self.title_format.format(secu_sht, rat_desc)
        item['Title'] = title

        sql = '''select Close, ChangePercActual from {} where InnerCode = {} and Date <= '{}' order by Date desc limit 1; 
        '''.format(self.idx_table, inner_code, self.day)    # 因为假如今天被机构首次评级 最新拿到的是昨天的行情数据
        ret = self.dc_client.select_one(sql)
        _close = self.re_decimal_data(ret.get("Close"))
        changepercactual = self.re_decimal_data(ret.get("ChangePercActual"))
        # 4月12日杰瑞股份（000021）获得申港证券首次评级 - 买入，最新收盘价24.86，涨幅+1.12%。
        # '{}月{}日{}（{}）获得{}首次评级 - {}，最新收盘价{}，涨幅{}%。'
        content = self.content_format.format(self.day.month, self.day.day, secu_sht, trd_code, com_name, rat_desc, _close, changepercactual)
        item['Content'] = content
        return item

    def start(self):
        datas = self.get_pub_today()
        self._dc_init()
        for data in datas:
            is_first = self.check_pub_first(data)
            if is_first:
                item = self.get_item(data)


if __name__ == "__main__":
    oe = OrganizationEvaluation()
    oe.start()
