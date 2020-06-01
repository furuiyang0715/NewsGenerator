import datetime
import json
import pprint
import struct

from PyAPI.JZpyapi import const
from PyAPI.JZpyapi.apis.report import Rank
from PyAPI.JZpyapi.client import SyncSocketClient
from base import NewsBase, logger
from configs import API_HOST, AUTH_USERNAME, AUTH_PASSWORD


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

    def _dc_init(self):
        self.dc_client = self._init_pool(self.dc_cfg)

    def __del__(self):
        if self.dc_client:
            self.dc_client.dispose()
        # if self.target_client:
        #     self.target_client.dispose()

    def get_changepercactual(self, secu_code):
        inner_code = self.get_inner_code_bysecu(secu_code)
        self._dc_init()
        sql = '''select Close, ChangePercActual from {} where InnerCode = {} and Date <= '{}' order by Date desc limit 1; 
        '''.format(self.idx_table, inner_code, self.day)    # 因为假如今天被机构首次评级立即发布,未收盘前拿到的是昨天的行情数据, 收盘手拿到的是今天的
        ret = self.dc_client.select_one(sql)
        changepercactual = self.re_decimal_data(ret.get("ChangePercActual"))
        return inner_code, changepercactual

    def start(self):
        rank_map = {}
        rank_num = 1
        rank = Rank.sync_get_rank_net_purchase_by_code_3_day(
            self.client, offset=0, count=10, stock_code_array=["$$沪深A股"]
        )
        for one in rank.row:
            item = {}
            logger.info("code: {}".format(one.stock_code))
            for i in one.data:
                if i.type == 1:
                    logger.info("value: {}".format(struct.unpack("<f", i.value)[0]))
                    secu_code = one.stock_code[2:]
                    inner_code, _changepercactual = self.get_changepercactual(secu_code)
                    item['value'] = struct.unpack("<f", i.value)[0]
                    item['secu_code'] = secu_code
                    item['inner_code'] = inner_code
                    item['changepercactual'] = _changepercactual
                    # print(type(_changepercactual))
                    rank_map[rank_num] = item
                    rank_num += 1
                elif i.type == 3:
                    logger.info(bytes.fromhex(i.value.hex()).decode("utf-8"))

        # print(pprint.pformat(rank_map))
        rank_info = json.dumps(rank_map, ensure_ascii=False)
        data = {"Date": self.day, "RankInfo": rank_info}
        # for k, v in data.items():
        #     print(k, ">>", v)


if __name__ == "__main__":
    mo = Stocks3DaysTop10()
    mo.start()
