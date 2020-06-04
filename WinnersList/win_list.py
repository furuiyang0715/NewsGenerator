


'''
龙虎榜-机构净买额最大
条件：龙虎榜机构净买额最大个股，收盘时发布
标题：今日机构净买额最多个股为宁德时代，机构净买额达10.04亿，总净买额达12.07亿。
内容：截至今日收盘，宁德时代机构净买额10.04亿，总净买金额达12.07亿，今日收盘价128.21，涨幅/跌幅+3.12%。
'''

# fupang专用
# 龙虎榜-主力净买额数据 && 龙虎榜-机构席位
## return: repeat xwmm_vary_data
## message xwmm_vary_data
## {
##     optional string code = 1;        // 股票代码
##     optional uint64 time = 2;        // 异动时间
##     optional double rise_rate = 3;  // 当日涨幅
##     optional double close = 4;      // 当日价格
##     repeated int32 abn_type = 5;   // 异动类型 [展示的时候进行列展开]
##     optional double tnv_rate = 6;   // 换收率
##     optional double net_buy = 7;    // 净买入
##     optional double sum_buy = 8;    // 总买入
##     optional double buy_rate = 9;   // 买入占比
##     optional double sum_sell = 10;  // 总卖出
##     optional double sell_rate = 11; // 卖出占比
##    optional double tnv_val =  12;  //  成交额
##     optional int32 org_count = 13; // 机构数量
##     optional double org_net_buy = 14; // 机构净买
##    repeated int32 faction_operator = 15; // 帮派操作; 1 联营 2 协同
##     repeated string faction_join = 16; // 参与帮派
##     optional double faction_net_buy = 17; // 帮派净买
##     optional double lgt_net_buy = 18;  // 陆股通净买
##     optional int32 abn_day = 19; // 异动天数 1,3
##     optional string industry_block_code = 20; // 所属行业板块代码
###    optional string industry_block_name = 21; // 所属行业板块名称
## }

import datetime
import time

from PyAPI.JZpyapi import const
from PyAPI.JZpyapi.apis.report import Finance
from PyAPI.JZpyapi.client import SyncSocketClient
from configs import API_HOST, AUTH_USERNAME, AUTH_PASSWORD

client = SyncSocketClient(
    API_HOST,
    6700,
    auth_username=AUTH_USERNAME,
    auth_password=AUTH_PASSWORD,
    login_on_connected=True,
    auth_type=const.AUTH_TYPE_CLIENT,
    max_retry=-1,
    # heartbeat=3,
)


test_time = (
    datetime.datetime.now() - datetime.timedelta(days=1) - datetime.timedelta(hours=2)
)
timestamp = int(time.mktime(test_time.timetuple()))
res = Finance.sync_get_vary(client, time=timestamp)
fields = ['code',    # 股票代码
          'time',   # 异动时间
          'rise_rate',   # 当日涨幅
          'close',  # 当日价格
          'org_net_buy',    # 机构净买
          'net_buy',   # 总净买入

          'org_count',  # 机构数量
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


print(max_netbuy_item)
print(max_orgcount_item)
