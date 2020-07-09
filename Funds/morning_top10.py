import datetime
import os
import pprint
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
from configs import API_HOST, AUTH_USERNAME, AUTH_PASSWORD, LOCAL


class MorningTop10(NewsBase):
    """早盘主力十大净买个股"""
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
        self.day = datetime.datetime.combine(datetime.datetime.now(), datetime.time.min)
        # self.target_table = 'news_generate_morningtop10'
        # self.fields = ["Date", "Title", "Content"]
        self.target_table = 'news_generate'
        self.fields = ['Date', 'Title', 'Content', 'NewsType', 'NewsJson']

    def _create_table(self):
        """
        新闻类型：
        1:  三日连续净流入前10个股
        2:  连板股今日竞价表现
        3: 早盘主力十大净买个股
        """
        self._target_init()
        sql = '''
        CREATE TABLE IF NOT EXISTS `{}` (
          `id` int(11) NOT NULL AUTO_INCREMENT,
          `NewsType` int NOT NULL COMMENT '新闻类型',
          `Date` datetime NOT NULL COMMENT '日期', 
          `NewsJson` json  DEFAULT  NULL COMMENT 'json 格式的新闻数据体', 
          `Title` varchar(64) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL COMMENT '生成文章标题', 
          `Content` text CHARACTER SET utf8 COLLATE utf8_bin COMMENT '生成文章正文',           
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
           PRIMARY KEY (`id`),
           UNIQUE KEY `dt_type` (`Date`, `NewsType`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='资讯生成表';
        '''.format(self.target_table)
        self.target_client.insert(sql)
        self.target_client.end()

    def dynamic_get_rank10(self):
        """早盘十大主力净买个股中去掉中文简称以 N 开头的个股
        比较严谨的做法是动态多次请求
        为了减少请求次数 比较粗暴地请求 50 个 从其中筛选出 10 个
        """
        items = self.get_rank10()
        processed_items = []
        count = 0
        for item in items:
            if not item["secu_abbr"].startswith("N"):
                processed_items.append(item)
                count += 1
                if count == 10:
                    break

        return processed_items

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

            item["main_buy"] = self.re_money_data(main_buy)
            item['rise_percent'] = self.re_decimal_data(rise_percent)
            item['rank_num'] = num
            num += 1
            items.append(item)

        return items

    def get_content(self, datas: list):
        final = dict()
        final["Date"] = self.day

        _month = self.day.month
        _day = self.day.day
        title = "{}月{}日早盘，十大主力净买个股（截取今日10点半前数据）".format(_month, _day)
        final['Title'] = title

        base_content = '{}月{}日大单金额流入前十名个股如下，数据取自（{}月{}日 10:30）\n'.format(_month, _day, _month, _day)

        for data in datas:
            content = '{}（{}）{}%，主力净买额{}\n'.format(data.get("secu_abbr"), data.get("secu_code")[2:], data.get("rise_percent"), data.get("main_buy"))
            base_content += content

        final["Content"] = base_content

        final['NewsType'] = 3
        return final

    def start(self):
        is_trading = self.is_trading_day(self.day)
        if not is_trading:
            logger.warning("非交易日")
            return

        if LOCAL:
            self._create_table()

        top10info = self.dynamic_get_rank10()

        to_insert = self.get_content(top10info)

        ret = self._save(self.target_client, to_insert, self.target_table, self.fields)

        self.ding("早盘十大成交股资讯生成\n {}".format(pprint.pformat(to_insert)))


def task():
    morn = MorningTop10()
    morn.start()


if __name__ == "__main__":
    # task()
    schedule.every().day.at("10:30").do(task)

    while True:
        schedule.run_pending()
        time.sleep(10)


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


'''进入根目录下进行部署 
docker build -f DockerfileUseApi2p -t registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/newsgenerator:v2 .
docker push registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/newsgenerator:v2 
sudo docker pull registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/newsgenerator:v2

sudo docker run --log-opt max-size=10m --log-opt max-file=3 -itd \
--env LOCAL=0 \
--name generate_morningtop10 \
registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/newsgenerator:v2 \
python Funds/morning_top10.py
'''
