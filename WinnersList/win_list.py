'''
龙虎榜-机构净买额最大
条件：龙虎榜机构净买额最大个股，收盘时发布
标题：今日机构净买额最多个股为宁德时代，机构净买额达10.04亿，总净买额达12.07亿。
内容：截至今日收盘，宁德时代机构净买额10.04亿，总净买金额达12.07亿，今日收盘价128.21，涨幅/跌幅+3.12%。

龙虎榜-机构席位最多
条件：龙虎榜机构买席数量最多个股，收盘时发布
标题：龙虎榜今日机构买入席位最多个股为山河药辅
内容：今日龙虎榜机构买席最多个股为山河药辅，10个机构买入，6个机构卖出，购买金额为10.12亿。今日收盘价128.21，涨幅/跌幅+3.12%。

fupang专用
龙虎榜-主力净买额数据 && 龙虎榜-机构席位
return: repeat xwmm_vary_data
message xwmm_vary_data
{
    optional string code = 1;        // 股票代码
    optional uint64 time = 2;        // 异动时间
    optional double rise_rate = 3;  // 当日涨幅
    optional double close = 4;      // 当日价格
    repeated int32 abn_type = 5;   // 异动类型 [展示的时候进行列展开]
    optional double tnv_rate = 6;   // 换收率
    optional double net_buy = 7;    // 净买入
    optional double sum_buy = 8;    // 总买入
    optional double buy_rate = 9;   // 买入占比
    optional double sum_sell = 10;  // 总卖出
    optional double sell_rate = 11; // 卖出占比
   optional double tnv_val =  12;  //  成交额
    optional int32 org_count = 13; // 机构数量
    optional double org_net_buy = 14; // 机构净买
   repeated int32 faction_operator = 15; // 帮派操作; 1 联营 2 协同
    repeated string faction_join = 16; // 参与帮派
    optional double faction_net_buy = 17; // 帮派净买
    optional double lgt_net_buy = 18;  // 陆股通净买
    optional int32 abn_day = 19; // 异动天数 1,3
    optional string industry_block_code = 20; // 所属行业板块代码
#    optional string industry_block_name = 21; // 所属行业板块名称
}

'''

import datetime
import os
import pprint
import sys
import time

from PyAPI.JZpyapi import const
from PyAPI.JZpyapi.apis.report import Finance
from PyAPI.JZpyapi.client import SyncSocketClient

cur_path = os.path.split(os.path.realpath(__file__))[0]
file_path = os.path.abspath(os.path.join(cur_path, ".."))
sys.path.insert(0, file_path)

from base import NewsBase, logger
from configs import API_HOST, AUTH_USERNAME, AUTH_PASSWORD, LOCAL


class WinList(NewsBase):
    def __init__(self):
        super(WinList, self).__init__()
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
        # self.get_time = datetime.datetime.now()
        # 收盘后接口才有数据
        self.target_table = 'news_generate_winlist'
        self.day = datetime.datetime(2020, 6, 4)
        self.get_time = datetime.datetime(2020, 6, 4, 15, 3)
        self.fields = ['PubDate', 'PubType', 'Title', 'Content']

    def _create_table(self):
        sql = '''
        CREATE TABLE IF NOT EXISTS `{}` (
          `id` int(11) NOT NULL AUTO_INCREMENT,
          `PubDate` datetime NOT NULL COMMENT '资讯发布时间', 
          `PubType` int NOT NULL COMMENT '资讯类型1:机构净买额最大2:机构购买席位数量最多',
          `Title` varchar(64) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL COMMENT '生成文章标题', 
          `Content` text CHARACTER SET utf8 COLLATE utf8_bin COMMENT '生成文章正文',
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
           PRIMARY KEY (`id`),
           UNIQUE KEY `un2` (`PubDate`, `PubType`) 
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='龙虎榜资讯生成';
        '''.format(self.target_table)
        self._target_init()
        self.target_client.insert(sql)
        logger.info("建表成功 ")

    def get_result(self):
        timestamp = int(time.mktime(self.get_time.timetuple()))
        res = Finance.sync_get_vary(self.client, time=timestamp)
        fields = ['code',    # 股票代码
                  'time',   # 异动时间
                  'rise_rate',   # 当日涨幅
                  'close',  # 当日价格
                  'org_net_buy',    # 机构净买
                  'net_buy',   # 总净买入  指标字段 1
                  'org_count',  # 机构数量 指标字段 2
                  ]

        max_one = None
        max_netbuy_item = dict()
        max_orgcount_item = dict()
        max_count = None
        for one in res.data:
            if max_one is None and max_count is None:
                max_one = one.org_net_buy
                max_count = one.org_count
                for field in fields:
                    max_netbuy_item[field] = getattr(one, field)
                    max_orgcount_item[field] = getattr(one, field)

            if max_one < one.org_net_buy:
                max_one = one.org_net_buy
                for field in fields:
                    max_netbuy_item[field] = getattr(one, field)

            if max_count < one.org_count:
                max_count = one.org_count
                for field in fields:
                    max_orgcount_item[field] = getattr(one, field)
        return max_netbuy_item, max_orgcount_item

    def gene_netbuy_data(self, ret):
        # print(ret)
        secu_addr = self.get_juyuan_codeinfo(ret.get("code")[2:])[1]
        org_net_buy = self.re_money_data(ret.get("org_net_buy"))
        net_buy = self.re_money_data(ret.get("net_buy"))
        close = self.re_decimal_data(ret.get("close"))
        rise_rate = self.re_decimal_data(ret.get("rise_rate"))
        title = '今日机构净买额最多个股为{}，机构净买额达{}，总净买额达{}。'.format(secu_addr, org_net_buy, net_buy)
        rise_str = "涨幅" if rise_rate > 0 else "跌幅"
        content = '截至今日收盘，{}机构净买额{}，总净买金额达{}，今日收盘价{}，{}{}%。'.format(
            secu_addr, org_net_buy, net_buy, close, rise_str, abs(rise_rate))
        # print(title)
        # print(content)
        final = dict()
        final['PubDate'] = self.day
        final['Title'] = title
        final['Content'] = content
        final['PubType'] = 1
        # print(pprint.pformat(final))
        return final

    def get_juyuan_codeinfo(self, secu_code):
        self._juyuan_init()
        sql = 'SELECT SecuCode,InnerCode, SecuAbbr from SecuMain WHERE SecuCategory in (1, 2, 8) \
and SecuMarket in (83, 90) \
and ListedSector in (1, 2, 6, 7) and SecuCode = "{}";'.format(secu_code)
        ret = self.juyuan_client.select_one(sql)
        return ret.get('InnerCode'), ret.get("SecuAbbr")

    def gene_orgcount_data(self, ret):
        # print(ret)
        secu_addr = self.get_juyuan_codeinfo(ret.get("code")[2:])[1]
        net_buy = self.re_money_data(ret.get("net_buy"))
        close = self.re_decimal_data(ret.get("close"))
        rise_rate = self.re_decimal_data(ret.get("rise_rate"))
        rise_str = "涨幅" if rise_rate > 0 else "跌幅"
        title = '龙虎榜今日机构买入席位最多个股为{}'.format(secu_addr)
        content = '今日龙虎榜机构买席最多个股为{}，10个机构买入，6个机构卖出，购买金额为{}。今日收盘价{}，{}{}%。'.format(
            secu_addr, net_buy, close, rise_str, rise_rate)
        final = dict()
        final['PubDate'] = self.day
        final['Title'] = title
        final['Content'] = content
        final['PubType'] = 2
        # print(pprint.pformat(final))
        return final

    def start(self):
        if LOCAL:
            self._create_table()

        ret1, ret2 = self.get_result()
        final1 = self.gene_netbuy_data(ret1)
        final2 = self.gene_orgcount_data(ret2)
        self._target_init()
        self._save(self.target_client, final1, self.target_table, self.fields)
        self._save(self.target_client, final2, self.target_table, self.fields)


if __name__ == "__main__":
    wl = WinList()
    wl.start()
