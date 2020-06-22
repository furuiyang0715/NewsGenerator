import datetime
import pprint
import struct
import sys

from PyAPI.JZpyapi import const
from PyAPI.JZpyapi.apis.report import Rank
from PyAPI.JZpyapi.client import SyncSocketClient
from base import NewsBase, logger
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

    def get_rank10(self):
        rank = Rank.sync_get_rank_net_purchase_by_code(
            self.client, offset=0, count=10, stock_code_array=["$$沪深A股"]
        )
        items = []
        num = 1

        for one in rank.row:
            item = dict()
            secu_code = one.stock_code
            item['secu_code'] = secu_code
            item['secu_abbr'] = self.get_juyuan_codeinfo(secu_code[2:])[1]
            entry = 0
            main_buy = None
            rise_percent = None

            for i in one.data:
                if i.type == 1:
                    if entry == 0:
                        main_buy = struct.unpack("<f", i.value)[0]
                    elif entry == 1:
                        rise_percent = struct.unpack("<f", i.value)[0]
                    entry += 1
                elif i.type == 3:
                    # print(bytes.fromhex(i.value.hex()).decode("utf-8"))
                    pass

            item["main_buy"] = main_buy
            item['rise_percent'] = rise_percent
            item['rank_num'] = num
            num += 1
            items.append(item)

        return items

    def get_content(self, datas: list):
        """
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
        """
        final = dict()
        _today = datetime.datetime.combine(datetime.datetime.now(), datetime.time.min)
        final["Date"] = _today
        _month = _today.month
        _day = _today.day

        title = "{}月{}日早盘，十大主力净买个股（截取今日10点半前数据）".format(_month, _day)
        final['Title'] = title
        base_content = '{}月{}日大单金额流入前十名个股如下，数据取自（{}月{}日 10:30）\n'.format(_month, _day, _month, _day)
        # {'secu_code': 'SZ002129', 'secu_abbr': '中环股份', 'main_buy': 688491328.0,
        # 'rise_percent': 10.025842666625977, 'rank_num': 1}
        # 山河药辅（300452）+1.54%，主力净买额1200万
        for data in datas:
            content = '{}（{}）{}%，主力净买额{}\n'.format(data.get("secu_abbr"), data.get("secu_code")[2:], data.get("rise_percent"), data.get("main_buy"))
            base_content += content

        final["Content"] = base_content
        return final

    def start(self):
        top10info = self.get_rank10()
        for one in top10info:
            print(one)

        to_insert = self.get_content(top10info)
        print(to_insert)


if __name__ == "__main__":
    morn = MorningTop10()
    morn.start()
