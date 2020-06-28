# 保存每日的主力十大净买股
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
from base import NewsBase
from configs import API_HOST, AUTH_USERNAME, AUTH_PASSWORD


class DayTopsSaver(NewsBase):
    """保存每日全部的流入排行数据【主力净买个股排行】
    TODO  这部分的数据存档似乎是给龙虎榜用的
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
            # heartbeat=3,
        )
        self.day = datetime.datetime.combine(datetime.datetime.today(), datetime.time.min)
        self.idx_table = 'stk_quot_idx'
        self.dc_client = None
        self.target_client = None
        self.juyuan_client = None
        self.target_table = 'rankdaytop'

    def _create_table(self):
        client = self._init_pool(self.product_cfg)
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
        client.insert(sql)
        client.dispose()

    def _dc_init(self):
        self.dc_client = self._init_pool(self.dc_cfg)

    def _target_init(self):
        self.target_client = self._init_pool(self.product_cfg)

    def _juyuan_init(self):
        self.juyuan_client = self._init_pool(self.juyuan_cfg)

    def __del__(self):
        if self.dc_client:
            self.dc_client.dispose()
        if self.target_client:
            self.target_client.dispose()
        if self.juyuan_client:
            self.juyuan_client.dispose()

    def get_juyuan_codeinfo(self, secu_code):
        self._juyuan_init()
        sql = 'SELECT SecuCode,InnerCode, SecuAbbr from SecuMain WHERE SecuCategory in (1, 2, 8) \
and SecuMarket in (83, 90) \
and ListedSector in (1, 2, 6, 7) and SecuCode = "{}";'.format(secu_code)
        ret = self.juyuan_client.select_one(sql)
        return ret.get('InnerCode'), ret.get("SecuAbbr")

    def get_changepercactual(self, inner_code):
        self._dc_init()
        sql = '''select Date, ChangePercActual from {} where InnerCode = '{}' and Date <= '{}' order by Date desc limit 1; 
        '''.format(self.idx_table, inner_code, self.day)  # 因为假如今天被机构首次评级立即发布,未收盘前拿到的是昨天的行情数据, 收盘手拿到的是今天的
        ret = self.dc_client.select_one(sql)
        changepercactual = self.re_decimal_data(ret.get("ChangePercActual"))
        print("&&&&&& ", ret.get("Date"))
        return changepercactual

    def start(self):
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
            # print("code:", one.stock_code)
            for i in one.data:
                if i.type == 1:
                    item = {}
                    secu_code = one.stock_code[2:]
                    # inner_code, secu_abbr = self.get_juyuan_codeinfo(secu_code)
                    # _changepercactual = self.get_changepercactual(inner_code)
                    item['value'] = struct.unpack("<f", i.value)[0]
                    item['secu_code'] = secu_code
                    # item['inner_code'] = inner_code
                    # item['secu_abbr'] = secu_abbr
                    # item['changepercactual'] = _changepercactual
                    rank_map[rank_num] = item
                    rank_num += 1
                elif i.type == 3:
                    print(bytes.fromhex(i.value.hex()).decode("utf-8"))

        # for k, v in rank_map.items():
            # print(k, ">>>", v)

        self._target_init()
        data = {"Date": self.day, "DayRank": json.dumps(rank_map, ensure_ascii=False)}
        self._save(self.target_client, data, self.target_table, ["Date", "DayRank"])
        self.ding('每日主力净买个股排行 json 数据已插入,数量{}'.format(len(list(rank_map.keys()))))


# if __name__ == "__main__":
#     DayTop10Saver().start()


def task():
    DayTopsSaver().start()


if __name__ == "__main__":
    task()
    schedule.every().day.at("03:05").do(task)

    while True:
        # print("当前调度系统中的任务列表 {}".format(schedule.jobs))
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
