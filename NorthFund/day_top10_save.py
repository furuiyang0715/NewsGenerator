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

        for one in rank.row:
            print("code:", one.stock_code)
            for i in one.data:
                if i.type == 1:
                    print("value:", struct.unpack("<f", i.value)[0])
                elif i.type == 3:
                    print(bytes.fromhex(i.value.hex()).decode("utf-8"))


if __name__ == "__main__":
    DayTop10Saver().start()
