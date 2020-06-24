'''
开盘异动盘口
条件：取主题猎手-盘口异动9：36分时，出现涨停个股且涨幅大于1.5%的异动盘口，(获取领涨股？ )再取两个涨幅最高的跟涨个股
标题：5G板块开盘活跃,涨幅高达1.5%
内容：
5G板块开盘活跃，盘口涨幅达1.5%，华星创业一字涨停，麦捷科技大涨9%，有方科技涨8%。


流程：
第一步: 拿到所有异动
第二步，查所有板块的实时涨幅，筛选出大于 1.5 的
第三步，把筛选完的数据，再根据个股的异动状态是涨停板或者封涨停板的，筛出来
第四步再去拿跟风股的实时涨幅

'''

import datetime
import pprint
import struct
import sys
import time
from collections import defaultdict

from PyAPI.JZpyapi import const
from PyAPI.JZpyapi.apis.report import TopicInvest, Rank
from PyAPI.JZpyapi.client import SyncSocketClient
from base import NewsBase


class OpenUnusual(NewsBase):
    def __init__(self):
        super(OpenUnusual, self).__init__()
        self.client = SyncSocketClient(
            # "192.168.0.241",
            "47.107.21.122",
            6700,
            auth_username="黄耿铿",
            auth_password="vhtyhr7c",
            login_on_connected=True,
            auth_type=const.AUTH_TYPE_CLIENT,
            max_retry=-1,
            # heartbeat=3,
        )

    def get_all_block_stats(self):
        base_time = datetime.datetime.now()
        target_time = base_time - datetime.timedelta(days=0,
                                                     # hours=8,
                                                     )
        now_ts = int(time.mktime(target_time.timetuple()))
        res = TopicInvest.sync_get_topic_info(self.client, ts=now_ts)

        # (1) 拿到全部的版块列表 并且保存全部的版块数据
        block_stats_map = defaultdict(list)
        # block_codes = []
        for block in res.msg_array:
            print(block)
            block_code = block.block_code
            # block_codes.append(block_code)

            stock_monitor = block.stock_monitor
            for one in stock_monitor:
                code = one.stock_code
                stats = one.status
                block_stats_map[block_code].append({"code": code, "stats": stats})

        print(pprint.pformat(block_stats_map))
        # print(block_codes)
        return block_stats_map

    def get_block_rise_map(self, all_block_codes):
        # (2) 查所有板块的实时涨幅，筛选出大于 1.5 的
        block_rise_map = {}
        rank = Rank.sync_get_rank_by_bk(self.client,
                                        offset=0,
                                        count=100,
                                        stock_code_array=all_block_codes
                                        )
        for one in rank.row:
            block_code = one.stock_code
            # print("code:", block_code)
            value = None
            for i in one.data:
                if i.type == 1:
                    value = struct.unpack("<f", i.value)[0]
                elif i.type == 3:
                    print(bytes.fromhex(i.value.hex()).decode("utf-8"))
                if value and value > 1.5:
                    block_rise_map[block_code] = value

        print(pprint.pformat(block_rise_map))
        return block_rise_map

    def start(self):
        '''
        enum stock_monitor_status
        {
        stock_monitor_status_null = 0;
        // 直线拉升
        stock_monitor_status_zxls_level_1 = 1;
        stock_monitor_status_zxls_level_2 = 2;
        // 疯涨停板
        stock_monitor_status_fztb_level = 3;
        // 涨停板
        stock_monitor_status_ztb_level = 4;
        // 冲高回落
        stock_monitor_status_cghl_level = 5;
        // 快速跳水
        stock_monitor_status_ksts_level_1 = 6;
        stock_monitor_status_ksts_level_2 = 7;
        stock_monitor_status_end = 1000;
        }
        '''
        all_block_stats_map = self.get_all_block_stats()
        all_block_codes = list(all_block_stats_map.keys())
        block_rise_map = self.get_block_rise_map(all_block_codes)
        rise_block_codes = list(block_rise_map.keys())
        print("# " * 20)
        final_items = {}
        for key, code_infos in all_block_stats_map.items():
            if key in rise_block_codes:
                final_items[key] = code_infos
                # for code_info in code_infos:
                #     if code_info.get("stats") in (3, 4):
                #         final_items[key] = code_infos
                #         break    # 退出内层循环

        print(pprint.pformat(final_items))
        if final_items:
            self.get_content(final_items, block_rise_map)

    def get_content(self, datas: dict, block_rise_map: dict):
        """
        {'IX850039': [{'code': 'SZ300459', 'stats': 3},
                      {'code': 'SH603003', 'stats': 1},
                      {'code': 'SH600070', 'stats': 1}],

        eg.
        标题：5G板块开盘活跃,涨幅高达1.5%
        内容：
        5G板块开盘活跃，盘口涨幅达1.5%，华星创业一字涨停，麦捷科技大涨9%，有方科技涨8%。
        """
        self._theme_init()
        for block_code, block_info in datas.items():
            sql = """select name from block where code = '{}';""".format(block_code)
            block_name = self.theme_client.select_one(sql)
            if block_name:
                block_name = block_name.get("name")
            else:
                raise
            block_rise = self.re_decimal_data(block_rise_map.get(block_code))
            title = '{}开盘活跃,涨幅高达{}%'.format(block_name, block_rise)
            count = 1
            base_content = ''
            for one in block_info:
                if count > 3:
                    break
                code = one.get("code")
                # 获取排在前面的两个的涨幅
                content = self.get_code_rise_info(code)
                base_content += content
                count += 1
            content = title + "," + base_content
            print(block_name)
            print(title)
            print(content)

    def get_code_rise_info(self, code):
        # 获取股票对应的涨跌幅信息


        return ''


if __name__ == "__main__":
    ou = OpenUnusual()
    ou.start()
