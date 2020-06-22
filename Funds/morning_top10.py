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

    @staticmethod
    def p_code(code):
        if code.startswith("6"):
            p_code = "SH" + code
        elif code.startswith("0") or code.startswith("3"):
            p_code = "SZ" + code
        else:
            return None
        return p_code

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

    # def update_real_risepercent(self, top10info: dict):
    #     """
    #     查询并且更入股票的实时涨跌幅
    #     """
    #     p_codes = [i[0] for i in top10info.values()]
    #     datas = []
    #     rank = Rank.sync_get_rank_by_rise_scope(self.client, stock_code_array=p_codes)
    #     for one in rank.row:
    #         data = dict()
    #         secu_code = one.stock_code[2:]
    #         data['secu_code'] = secu_code
    #         data['secu_abbr'] = self.get_juyuan_codeinfo(secu_code)[1]
    #         rise_p = None
    #         for i in one.data:
    #             if i.type == 1:
    #                 rise_p = struct.unpack("<f", i.value)[0]
    #             elif i.type == 4:
    #                 rise_p = struct.unpack("<i", i.value)[0]
    #             elif i.type == 2:
    #                 rise_p = struct.unpack("<d", i.value)[0]
    #             elif i.type == 3:
    #                 rise_p = bytes.fromhex(i.value.hex()).decode("utf-8")
    #             else:
    #                 raise ValueError("解析错误")
    #         data['rise_percent'] = rise_p
    #         topinfo = top10info.get(secu_code)
    #         data['rank'] = topinfo[1]
    #         data["main_buy_value"] = topinfo[2]
    #         datas.append(data)
    #     return datas

    # def get_content(self, datas):
    #     """
    #     早盘主力十大净买个股
    #     标题：4月15日早盘，十大主力净买个股（截取今日10点半前数据）
    #     条件：取每天十点半时，主力净买额前十的个股
    #
    #     内容：
    #     4月15日大单金额流入前十名个股如下，数据取自（4月15日 10:30）
    #     山河药辅（300452）+1.54%，主力净买额1200万    (实时涨跌幅)
    #     皇氏集团（002329）+1.54%，主力净买额1200万
    #     以岭药业（002603）+1.54%，主力净买额1200万
    #     海伦哲（300201）  +1.54%，主力净买额1200万
    #     宝色股份（300402）+1.54%，主力净买额1200万
    #     延江股份（300658）+1.54%，主力净买额1200万
    #     龙宇燃油（603003）+1.54%，主力净买额1200万
    #     泉峰汽车（603982）+1.54%，主力净买额1200万
    #     """
    #     _today_now = datetime.datetime.now()
    #     _month = _today_now.month
    #     _day = _today_now.day
    #     title = "{}月{}日早盘，十大主力净买个股（截取今日10点半前数据）".format(_month, _day)
    #     base_content = '{}月{}日大单金额流入前十名个股如下，数据取自（{}月{}日 10:30）\n'.format(_month, _day, _month, _day)
    #     for data in datas:
    #         content = '山河药辅（300452）+1.54%，主力净买额1200万'

    def start(self):
        top10info = self.get_rank10()
        for one in top10info:
            print(one)

        # to_insert = self.get_content(datas)


if __name__ == "__main__":
    morn = MorningTop10()
    morn.start()
