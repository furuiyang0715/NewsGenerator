# 开盘异动盘口
import datetime
import struct
import time

from PyAPI.JZpyapi import const
from PyAPI.JZpyapi.apis.report import Rank, TopicInvest
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


now_ts = int(time.mktime(datetime.datetime.now().timetuple()))
res = TopicInvest.sync_get_topic_info(client, ts=now_ts)
# print(res)

rank = Rank.sync_get_rank_by_bk(client, offset=0, count=1000)
count = 0
for one in rank.row:
    print("code:", one.stock_code)
    count += 1
    for i in one.data:
        if i.type == 1:
            print("value:", struct.unpack("<f", i.value)[0])
        elif i.type == 3:
            print(bytes.fromhex(i.value.hex()).decode("utf-8"))

print(count)
