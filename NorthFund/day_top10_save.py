# 保存每日的主力十大净买股
import datetime
import struct

from PyAPI.JZpyapi import const
from PyAPI.JZpyapi.apis.report import Rank
from PyAPI.JZpyapi.client import SyncSocketClient
from base import NewsBase
from configs import API_HOST, AUTH_USERNAME, AUTH_PASSWORD


class DayTop10Saver(NewsBase):
    def __init__(self):
        super(DayTop10Saver, self).__init__()
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
        self.day = datetime.datetime.combine(datetime.datetime.today(), datetime.time.min)
        self.idx_table = 'stk_quot_idx'
        self.dc_client = None
        self.target_client = None
        self.juyuan_client = None
        self.target_table = ''

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
        '''.format(self.idx_table, inner_code, self.day)  # 因为假如今天被机构首次评级立即发布,未收盘前拿到的是昨天的行情数据, 收盘手拿到的是今天的
        ret = self.dc_client.select_one(sql)
        changepercactual = self.re_decimal_data(ret.get("ChangePercActual"))
        print("&&&&&& ", ret.get("Date"))
        return changepercactual

    def start(self):
        _count = 4000
        # 在不知今天的具体有多少只的情况下, 拿到今天的全部数据
        while True:
            rank = Rank.sync_get_rank_net_purchase_by_code(
                self.client, offset=0, count=_count, stock_code_array=["$$沪深A股"]
            )

            print(len(rank.row))
            if len(rank.row) < _count:
                break
            else:
                _count += 100

        rank_map = {}
        rank_num = 1
        for one in rank.row:
            print("code:", one.stock_code)
            for i in one.data:
                if i.type == 1:
                    item = {}
                    secu_code = one.stock_code[2:]
                    inner_code, secu_abbr = self.get_juyuan_codeinfo(secu_code)
                    _changepercactual = self.get_changepercactual(inner_code)
                    item['value'] = struct.unpack("<f", i.value)[0]
                    item['secu_code'] = secu_code
                    item['inner_code'] = inner_code
                    item['secu_abbr'] = secu_abbr
                    item['changepercactual'] = _changepercactual
                    rank_map[rank_num] = item
                    rank_num += 1
                elif i.type == 3:
                    print(bytes.fromhex(i.value.hex()).decode("utf-8"))

        for k, v in rank_map.items():
            print(k, ">>>", v)


if __name__ == "__main__":
    DayTop10Saver().start()
