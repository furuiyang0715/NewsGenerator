import datetime
import json
import os
import struct
import sys
import time
import schedule

cur_path = os.path.split(os.path.realpath(__file__))[0]
file_path = os.path.abspath(os.path.join(cur_path, ".."))
sys.path.insert(0, file_path)
from base import NewsBase, logger
from configs import API_HOST, AUTH_USERNAME, AUTH_PASSWORD
from PyAPI.JZpyapi import const
from PyAPI.JZpyapi.apis.report import Rank
from PyAPI.JZpyapi.client import SyncSocketClient


_today = datetime.datetime.combine(datetime.datetime.today(), datetime.time.min)


class Stocks3DaysTop10(NewsBase):
    """三日连续净流入前10个股"""
    def __init__(self, day=_today):
        super(Stocks3DaysTop10, self).__init__()

        self.client = SyncSocketClient(
            API_HOST,
            6700,
            auth_username=AUTH_USERNAME,
            auth_password=AUTH_PASSWORD,
            login_on_connected=True,
            auth_type=const.AUTH_TYPE_CLIENT,
            max_retry=-1,
            # heartbeat=3,
        )
        self.day = day
        self.idx_table = 'stk_quot_idx'
        self.dc_client = None
        self.target_client = None
        self.juyuan_client = None
        self.target_table = 'news_generate_stocks3daystop10'

    def _dc_init(self):
        self.dc_client = self._init_pool(self.dc_cfg)

    def _target_init(self):
        self.target_client = self._init_pool(self.product_cfg)

    def _juyuan_init(self):
        self.juyuan_client = self._init_pool(self.juyuan_cfg)

    def __del__(self):
        if self.dc_client:
            self.dc_client.dispose()
        if self.target_client:
            self.target_client.dispose()
        if self.juyuan_client:
            self.juyuan_client.dispose()

    def _create_table(self):
        client = self._init_pool(self.product_cfg)
        sql = '''
        CREATE TABLE IF NOT EXISTS `{}` (
          `id` int(11) NOT NULL AUTO_INCREMENT,
          `Date` datetime NOT NULL COMMENT '日期', 
          `RankInfo` json  NOT NULL COMMENT '三日连续净流入前10个股', 
          `Title` varchar(64) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL COMMENT '生成文章标题', 
          `Content` text CHARACTER SET utf8 COLLATE utf8_bin COMMENT '生成文章正文',           
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
           PRIMARY KEY (`id`),
           UNIQUE KEY `dt_thre` (`Date`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='三日连续净流入前10个股';
        '''.format(self.target_table)
        client.insert(sql)
        client.dispose()

    def get_juyuan_codeinfo(self, secu_code):
        self._juyuan_init()
        sql = 'SELECT SecuCode,InnerCode, SecuAbbr from SecuMain WHERE SecuCategory in (1, 2, 8) \
and SecuMarket in (83, 90) \
and ListedSector in (1, 2, 6, 7) and SecuCode = "{}";'.format(secu_code)
        ret = self.juyuan_client.select_one(sql)
        return ret.get('InnerCode'), ret.get("SecuAbbr")

    def get_changepercactual(self, inner_code):
        self._dc_init()
        sql = '''select Date, ChangePercActual from {} where InnerCode = '{}' and Date <= '{}' order by Date desc limit 1; 
        '''.format(self.idx_table, inner_code, self.day)    # 因为假如今天被机构首次评级立即发布,未收盘前拿到的是昨天的行情数据, 收盘手拿到的是今天的
        ret = self.dc_client.select_one(sql)
        changepercactual = self.re_decimal_data(ret.get("ChangePercActual"))
        print("&&&&&& ", ret.get("Date"))
        return changepercactual

    def start(self):
        self._create_table()

        rank_map = {}
        rank_num = 1
        rank = Rank.sync_get_rank_net_purchase_by_code_3_day(
            self.client, offset=0, count=10, stock_code_array=["$$沪深A股"]
        )
        for one in rank.row:
            item = {}
            print("code: {}".format(one.stock_code))
            for i in one.data:
                if i.type == 1:
                    print("value: {}".format(struct.unpack("<f", i.value)[0]))
                    secu_code = one.stock_code[2:]
                    inner_code, secu_abbr = self.get_juyuan_codeinfo(secu_code)
                    _changepercactual = self.get_changepercactual(inner_code)
                    item['value'] = struct.unpack("<f", i.value)[0]
                    item['secu_code'] = secu_code
                    item['inner_code'] = inner_code
                    item['secu_abbr'] = secu_abbr
                    item['changepercactual'] = _changepercactual
                    # print(type(_changepercactual))
                    rank_map[rank_num] = item
                    rank_num += 1
                elif i.type == 3:
                    logger.info(bytes.fromhex(i.value.hex()).decode("utf-8"))

        rank_info = json.dumps(rank_map, ensure_ascii=False)

        content = '三日主力净买额前十的个股:\n'
        # eg. 山河药辅（300452）+1.54%，三日主力净买额1200万
        one_format = '{}（{}）{}%，三日主力净买额{}'

        for k, v in rank_map.items():
            changepercactual_str = ("+" + str(self.re_decimal_data(v.get("changepercactual")))
                                    if v.get("changepercactual") > 0 else "-" + str(self.re_decimal_data(v.get("changepercactual"))))
            value_str = self.re_money_data(v.get("value"))
            content += one_format.format(v.get('secu_abbr'), v.get('secu_code'), changepercactual_str, value_str) + "\n"

        title = '这些个股被主力看好，主力资金3日净流入前十个股'
        print(content)

        self._target_init()
        data = {"Date": self.day, "RankInfo": rank_info, 'Content': content, "Title": title}
        self._save(self.target_client, data, self.target_table, ['Date', "RankInfo"])


def task():
    mo = Stocks3DaysTop10()
    mo.start()


if __name__ == "__main__":
    task()
    schedule.every().day.at("03:05").do(task)

    while True:
        # print("当前调度系统中的任务列表 {}".format(schedule.jobs))
        schedule.run_pending()
        time.sleep(30)
