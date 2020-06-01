import struct

from PyAPI.JZpyapi import const
from PyAPI.JZpyapi.apis.report import Rank
from PyAPI.JZpyapi.client import SyncSocketClient
from base import NewsBase, logger
from configs import API_HOST, AUTH_USERNAME, AUTH_PASSWORD


class Stocks3DaysTop10(NewsBase):
    """三日连续净流入前 10 个股"""
    def __init__(self):
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

    def start(self):
        # 3日连续净流入前十个股
        # 测试用例:
        rank = Rank.sync_get_rank_net_purchase_by_code_3_day(
            self.client, offset=0, count=10, stock_code_array=["$$沪深A股"]
        )
        for one in rank.row:
            logger.info("code: {}".format(one.stock_code))
            for i in one.data:
                if i.type == 1:
                    logger.info("value: {}".format(struct.unpack("<f", i.value)[0]))
                elif i.type == 3:
                    logger.info(bytes.fromhex(i.value.hex()).decode("utf-8"))


if __name__ == "__main__":
    mo = Stocks3DaysTop10()
    mo.start()
