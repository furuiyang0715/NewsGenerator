import datetime
import json
import os
import pprint
import struct
import sys
import time
import schedule

cur_path = os.path.split(os.path.realpath(__file__))[0]
file_path = os.path.abspath(os.path.join(cur_path, ".."))
sys.path.insert(0, file_path)

from base import NewsBase, logger
from configs import API_HOST, AUTH_USERNAME, AUTH_PASSWORD, LOCAL
from PyAPI.JZpyapi import const
from PyAPI.JZpyapi.apis.report import Rank
from PyAPI.JZpyapi.client import SyncSocketClient


class Stocks3DaysTop10(NewsBase):
    """三日连续净流入前10个股"""
    def __init__(self):
        super(Stocks3DaysTop10, self).__init__()

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
        self.data_day = None  # 最近一次有数据的时间
        self.idx_table = 'stk_quot_idx'
        self.dc_client = None
        self.target_client = None
        self.juyuan_client = None
        # self.target_table = 'news_generate_stocks3daystop10'
        self.target_table = 'news_generate'

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

    def _create_table(self):
        """
        新闻类型：
        1： 三日连续净流入前10个股
        2：
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
        '''.format(self.idx_table, inner_code, self.day)    # 因为假如今天被机构首次评级立即发布,未收盘前拿到的是昨天的行情数据, 收盘后拿到的是今天的
        ret = self.dc_client.select_one(sql)
        changepercactual = self.re_decimal_data(ret.get("ChangePercActual"))
        print("&&&&&& ", ret.get("Date"))
        self.data_day = ret.get("Date")
        return changepercactual

    def start(self):
        is_trading = self.is_trading_day(self.day)
        if not is_trading:
            logger.warning("{} 非交易日".format(self.day))

        if LOCAL:
            self._create_table()

        rank_map = {}
        rank_num = 1
        rank = Rank.sync_get_rank_net_purchase_by_code_3_day(
            self.client, offset=0, count=10, stock_code_array=["$$沪深A股"]
        )
        for one in rank.row:
            item = {}
            print("code: {}".format(one.stock_code))
            for i in one.data:
                if i.type == 1:
                    print("value: {}".format(struct.unpack("<f", i.value)[0]))
                    secu_code = one.stock_code[2:]
                    inner_code, secu_abbr = self.get_juyuan_codeinfo(secu_code)
                    _changepercactual = self.get_changepercactual(inner_code)
                    item['value'] = struct.unpack("<f", i.value)[0]
                    item['secu_code'] = secu_code
                    item['inner_code'] = inner_code
                    item['secu_abbr'] = secu_abbr
                    item['changepercactual'] = _changepercactual
                    # print(type(_changepercactual))
                    rank_map[rank_num] = item
                    rank_num += 1
                elif i.type == 3:
                    logger.info(bytes.fromhex(i.value.hex()).decode("utf-8"))

        rank_info = json.dumps(rank_map, ensure_ascii=False)

        content = '三日主力净买额前十的个股:\n'
        # eg. 山河药辅（300452）+1.54%，三日主力净买额1200万
        one_format = '{}（{}）{}%，三日主力净买额{}'

        for k, v in rank_map.items():
            changepercactual_str = ("+" + str(self.re_decimal_data(v.get("changepercactual")))
                                    if v.get("changepercactual") > 0 else "-" + str(self.re_decimal_data(v.get("changepercactual"))))
            value_str = self.re_money_data(v.get("value"))
            content += one_format.format(v.get('secu_abbr'), v.get('secu_code'), changepercactual_str, value_str) + "\n"

        title = '这些个股被主力看好，主力资金3日净流入前十个股'
        print(content)

        data = {
            # "Date": self.day,
            "Date": self.data_day,
            "NewsType": 1,
            "NewsJson": rank_info,
            'Content': content,
            "Title": title,
        }
        print(data)
        self._target_init()
        ret = self._save(self.target_client, data, self.target_table, ['Date', "NewsType", "NewsJson", 'Content', "Title"])
        self.ding("主力资金3日净流入前十个股-资讯生成\n{}".format(pprint.pformat(data)))


def task():
    mo = Stocks3DaysTop10()
    mo.start()


if __name__ == "__main__":
    # task()
    schedule.every().day.at("15:05").do(task)

    while True:
        # print("当前调度系统中的任务列表 {}".format(schedule.jobs))
        schedule.run_pending()
        time.sleep(30)

'''
3日净流入前十个股
条件：三个交易日主力净买额前十的个股，每日收盘发布
标题：这些个股被主力看好，主力资金3日净流入前十个股

内容：
三日主力净买额前十的个股：

山河药辅（300452）+1.54%，三日主力净买额1200万
皇氏集团（002329）+1.54%，三日主力净买额1200万
以岭药业（002603）+1.54%，三日主力净买额1200万
海伦哲（300201）  +1.54%，三日主力净买额1200万
宝色股份（300402）+1.54%，三日主力净买额1200万
延江股份（300658）+1.54%，三日主力净买额1200万
龙宇燃油（603003）+1.54%，三日主力净买额1200万
泉峰汽车（603982）+1.54%，三日主力净买额1200万
'''


'''进入根目录下进行部署 
docker build -f DockerfileUseApi2p -t registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/newsgenerator:v2 . 
docker push registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/newsgenerator:v2 
sudo docker pull registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/newsgenerator:v2

sudo docker run --log-opt max-size=10m --log-opt max-file=3 -itd \
--env LOCAL=0 \
--name generate_days3top10 \
registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/newsgenerator:v2 \
python Funds/days3_top10.py
'''
