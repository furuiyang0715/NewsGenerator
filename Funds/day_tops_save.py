import datetime
import json
import os
import struct
import sys
import time

import schedule
cur_path = os.path.split(os.path.realpath(__file__))[0]
file_path = os.path.abspath(os.path.join(cur_path, ".."))
sys.path.insert(0, file_path)

from PyAPI.JZpyapi import const
from PyAPI.JZpyapi.apis.report import Rank
from PyAPI.JZpyapi.client import SyncSocketClient
from base import NewsBase, logger
from configs import API_HOST, AUTH_USERNAME, AUTH_PASSWORD


class DayTopsSaver(NewsBase):
    """保存每日全部的流入排行数据【主力净买个股排行】
    该数据是保存在爬虫库还是正式的 dc 库看情况去修改对应的配置
    """
    def __init__(self):
        super(DayTopsSaver, self).__init__()
        self.client = SyncSocketClient(
            API_HOST,
            6700,
            auth_username=AUTH_USERNAME,
            auth_password=AUTH_PASSWORD,
            login_on_connected=True,
            auth_type=const.AUTH_TYPE_CLIENT,
            max_retry=-1,
        )
        # 请求发起的时间
        self.day = datetime.datetime.combine(datetime.datetime.today(), datetime.time.min)
        # 最近有数据的时间
        self.data_day = None
        # dc 中的行情数据表
        self.idx_table = 'stk_quot_idx'
        # 保存每日全部的流入排行数据的表名
        self.target_table = 'rankdaytop'

    def _create_table(self):
        self._target_init()
        sql = '''
        CREATE TABLE IF NOT EXISTS `{}` (
          `id` int(11) NOT NULL AUTO_INCREMENT,
          `Date` datetime NOT NULL COMMENT '日期', 
          `DayRank` json  NOT NULL COMMENT '每日主机净买排行 json', 
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
           PRIMARY KEY (`id`),
           UNIQUE KEY `dt_thre` (`Date`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='每日净流入排行';
        '''.format(self.target_table)
        self.target_client.insert(sql)
        self.target_client.end()

    def start(self):
        # 判断是否是交易日
        is_trading = self.is_trading_day(self.day)
        if not is_trading:
            logger.warning("非交易日")
            return

        # 建表
        self._create_table()

        _count = 4000
        # 在不知今天的具体有多少只的情况下, 拿到今天的全部数据
        while True:
            rank = Rank.sync_get_rank_net_purchase_by_code(
                self.client, offset=0, count=_count, stock_code_array=["$$沪深A股"]
            )

            print(len(rank.row))
            if len(rank.row) < _count:
                break
            else:
                _count += 100

        rank_map = {}
        rank_num = 1
        for one in rank.row:
            for i in one.data:
                if i.type == 1:
                    item = {}
                    secu_code = one.stock_code[2:]
                    item['value'] = struct.unpack("<f", i.value)[0]
                    item['secu_code'] = secu_code
                    rank_map[rank_num] = item
                    rank_num += 1
                elif i.type == 3:
                    print(bytes.fromhex(i.value.hex()).decode("utf-8"))

        data = {"Date": self.data_day, "DayRank": json.dumps(rank_map, ensure_ascii=False)}
        self._save(self.target_client, data, self.target_table, ["Date", "DayRank"])
        self.ding('每日主力净买个股排行 json 数据已插入,数量{}'.format(len(list(rank_map.keys()))))


def task():
    DayTopsSaver().start()


if __name__ == "__main__":
    task()
    schedule.every().day.at("15:05").do(task)

    while True:
        schedule.run_pending()
        time.sleep(30)


'''进入跟目录执行 保存每日流入排行数据
docker build -f DockerfileUseApi2p -t registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/newsgenerator:v2 . 
docker push registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/newsgenerator:v2  
sudo docker pull registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/newsgenerator:v2
 
sudo docker run --log-opt max-size=10m --log-opt max-file=3 -itd \
--env LOCAL=0 \
--name save_daytop \
registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/newsgenerator:v2 \
python Funds/day_tops_save.py

'''
