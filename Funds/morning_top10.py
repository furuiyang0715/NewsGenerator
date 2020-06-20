'''
早盘主力十大净买个股
标题：4月15日早盘，十大主力净买个股（截取今日10点半前数据）
条件：取每天十点半时，主力净买额前十的个股

内容：
4月15日大单金额流入前十名个股如下，数据取自（4月15日 10:30）
山河药辅（300452）+1.54%，主力净买额1200万    (实时涨跌幅)
皇氏集团（002329）+1.54%，主力净买额1200万
以岭药业（002603）+1.54%，主力净买额1200万
海伦哲（300201）  +1.54%，主力净买额1200万
宝色股份（300402）+1.54%，主力净买额1200万
延江股份（300658）+1.54%，主力净买额1200万
龙宇燃油（603003）+1.54%，主力净买额1200万
泉峰汽车（603982）+1.54%，主力净买额1200万

'''
import struct

from PyAPI.JZpyapi import const
from PyAPI.JZpyapi.apis.report import Rank
from PyAPI.JZpyapi.client import SyncSocketClient
from base import NewsBase
from configs import API_HOST, AUTH_USERNAME, AUTH_PASSWORD


class MorningTop10(NewsBase):
    def __init__(self):
        super(MorningTop10, self).__init__()
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
        # 在 10 点半时间进行请求
        rank = Rank.sync_get_rank_net_purchase_by_code(
            self.client, offset=0, count=10, stock_code_array=["$$沪深A股"]
        )

        rank_num = 1
        for one in rank.row:
            item = {}
            for i in one.data:
                if i.type == 1:
                    secu_code = one.stock_code[2:]
                    item['secu_code'] = secu_code
                    item['value'] = struct.unpack("<f", i.value)[0]
                    item['rank_num'] = rank_num
                elif i.type == 3:
                    print(bytes.fromhex(i.value.hex()).decode("utf-8"))
            rank_num += 1

            print(item)    # {'secu_code': '300059', 'value': 7.079639434814453, 'rank_num': 1}


if __name__ == "__main__":
    morn = MorningTop10()
    morn.start()
