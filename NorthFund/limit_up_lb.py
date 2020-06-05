# 连板股今日竞价表现
import struct

from PyAPI.JZpyapi import const
from PyAPI.JZpyapi.apis.report import Rank
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

rank = Rank.sync_get_limit_up_lb_count(
    client,
    offset=0,
    count=1000,
    stock_code_array=["$$主题猎手-昨日涨停"]
    # client,
    # offset=0,
    # count=10,
    # stock_code_array=["$$今日涨停"],
)
# 返回的value从上往下依次是:连板数量,涨停封板金额,涨幅,涨停板成交额,最新价，涨停价，跌停价，更新时间
fields = ["lb_count",  # 连板数量
          'rise_close_amount',  # 涨停封板金额
          'rise_scope',  # 涨幅
          'limit_up_amount',  # 涨停板成交额
          'current_price',  # 最新价
          'limit_up_price',  # 涨停价
          'limit_down_price',  # 跌停价
          'update_time',  # 更新时间
          'code',  # 证券代码
          ]
items = []
for one in rank.row:
    code = one.stock_code
    values = []
    for i in one.data:
        if i.type == 1:
            value = struct.unpack("<f", i.value)[0]
        elif i.type == 4:
            value = struct.unpack("<i", i.value)[0]
        elif i.type == 2:
            value = struct.unpack("<d", i.value)[0]
        elif i.type == 3:
            value = bytes.fromhex(i.value.hex()).decode("utf-8")
        else:
            raise ValueError

        # print(value)
        values.append(value)

    values.append(code)
    item = dict(zip(fields, values))
    items.append(item)
    if item.get("lb_count") < 2:
        break


print(len(items))
