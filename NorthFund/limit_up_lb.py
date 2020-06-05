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
    count=10,
    stock_code_array=["$$主题猎手-昨日涨停"]

    # client,
    # offset=0,
    # count=10,
    # stock_code_array=["$$今日涨停"],
)
# 返回的value从上往下依次是:连板数量,涨停封板金额,涨幅,涨停板成交额
for one in rank.row:
    print()
    print("code:", one.stock_code)
    for i in one.data:
        if i.type == 1:
            print("value1:", struct.unpack("<f", i.value)[0])
        elif i.type == 4:
            print("value4:", struct.unpack("<i", i.value)[0])
        elif i.type == 2:
            print("value2:", struct.unpack("<d", i.value)[0])
        elif i.type == 3:
            print(bytes.fromhex(i.value.hex()).decode("utf-8"))