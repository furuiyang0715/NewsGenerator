import datetime
import os
import pprint
import struct
import sys
import time
from collections import defaultdict

import schedule

cur_path = os.path.split(os.path.realpath(__file__))[0]
file_path = os.path.abspath(os.path.join(cur_path, ".."))
sys.path.insert(0, file_path)

from configs import API_HOST, AUTH_USERNAME, AUTH_PASSWORD
from PyAPI.JZpyapi import const
from PyAPI.JZpyapi.apis.report import TopicInvest, Rank
from PyAPI.JZpyapi.client import SyncSocketClient
from base import NewsBase, logger


class OpenUnusual(NewsBase):
    def __init__(self):
        super(OpenUnusual, self).__init__()
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
        self.base_time = datetime.datetime.now()
        self.day = datetime.datetime.combine(self.base_time, datetime.time.min)
        # self.stock_monitor_status_map = {
        #     1: "直线拉升",
        #     2: "直线拉升",
        #     3: "疯涨停板",
        #     4: "涨停板",
        #     5: "冲高回落",
        #     6: "快速跳水",
        #     7: "快速跳水",
        # }
        # self.target_table = 'news_generate_openunusual'
        self.target_table = 'news_generate'

    def history(self):
        """
        历史数据的生成
        """
        start_day = datetime.datetime(2020, 1, 1)
        end_day = datetime.datetime(2020, 6, 29)
        _day = start_day
        while _day <= end_day:
            # 判断当天是否是交易日
            is_trading = self.is_trading_day(_day)
            if not is_trading:
                logger.warning("{}非交易日".format(_day))
            else:
                all_block_stats_map = self.get_all_block_stats(_day)
                if not all_block_stats_map:
                    logger.warning("未获取到交易日 {} 的 block 信息".format(_day))
                else:
                    all_block_codes = list(all_block_stats_map.keys())
                    block_rise_map = self.get_block_rise_map(all_block_codes)
                    final = self.get_content(all_block_stats_map, block_rise_map)
                    if not final:
                        logger.warning("今日{}无数据生成".format(_day))
                    else:
                        ret = self._save(self.target_client, final, self.target_table,
                                         ['Title', 'Date', 'Content', 'NewsType', 'NewsJson'])

            _day += datetime.timedelta(days=1)

    def get_all_block_stats(self, req_day):
        """
        获取某个请求时间点的版块数据
        """
        _year, _month, _day = req_day.year, req_day.month, req_day.day
        target_time = datetime.datetime(_year, _month, _day, 9, 36, 0)
        now_ts = int(time.mktime(target_time.timetuple()))
        res = TopicInvest.sync_get_topic_info(self.client, ts=now_ts)

        # 拿到全部的版块列表 并且保存全部的版块数据
        block_stats_map = defaultdict(list)
        for block in res.msg_array:
            block_code = block.block_code
            stock_monitor = block.stock_monitor
            for one in stock_monitor:
                code = one.stock_code
                stats = one.status
                block_stats_map[block_code].append({"code": code, "stats": stats})
        return block_stats_map

    def get_block_rise_map(self, all_block_codes):
        """
        查所有板块的实时涨幅，筛选出大于 1.5 的
        """
        block_rise_map = {}
        rank = Rank.sync_get_rank_by_bk(self.client,
                                        offset=0,
                                        count=100,
                                        stock_code_array=all_block_codes
                                        )
        for one in rank.row:
            block_code = one.stock_code
            value = None
            for i in one.data:
                if i.type == 1:
                    value = struct.unpack("<f", i.value)[0]
                elif i.type == 3:
                    print(bytes.fromhex(i.value.hex()).decode("utf-8"))
                if value and value > 1.5:
                    block_rise_map[block_code] = value
        return block_rise_map

    def get_content(self, datas, block_rise_map):
        """
        {'IX850039': [{'code': 'SZ300459', 'stats': 3},
                      {'code': 'SH603003', 'stats': 1},
                      {'code': 'SH600070', 'stats': 1}],
                      ......


        eg.
        标题：5G板块开盘活跃,涨幅高达1.5%
        内容：
        5G板块开盘活跃，盘口涨幅达1.5%，华星创业一字涨停，麦捷科技大涨9%，有方科技涨8%。
        """
        self._theme_init()
        block_names = []
        rows_str = ''
        for block_code, block_info in datas.items():
            if block_code in block_rise_map:
                sql = """select name from block where code = '{}';""".format(block_code)
                block_name = self.theme_client.select_one(sql)
                if block_name:
                    block_name = block_name.get("name")
                    block_names.append(block_name)
                else:
                    raise
                block_rise = self.re_decimal_data(block_rise_map.get(block_code))
                count = 1
                base_content = '{}版块竞价涨幅达{}%，'.format(block_name, block_rise)
                for one in block_info:
                    if count > 3:
                        break
                    code = one.get("code")
                    stats = one.get("stats")
                    if count == 1:    # 首次循环出的是领涨股 之后的是跟涨股
                        content = self.get_code_rise_info(code, stats, lead=1)
                    else:
                        content = self.get_code_rise_info(code, stats, lead=0)
                    base_content += content
                    count += 1

                # ，后面有个空格
                row = base_content[:-2] + "。\n"
                rows_str += row

        if not block_names:
            logger.warning("今日无数据生成")
            return

        title_format = "今日竞价表现：" + ("{}、"*len(block_names))[:-1] + "板块活跃"
        title = title_format.format(*block_names)
        final = dict()
        final['Title'] = title
        final['Content'] = rows_str
        final['Date'] = self.day
        final['NewsType'] = 4
        return final

    def get_code_rise_info(self, code, stats, lead=1):
        """
        TODO
        查询股票的实时涨跌幅, 该接口在非交易日也存在数据.
        lead 表示是否领涨股.
        """
        rank = Rank.sync_get_rank_by_rise_scope(
            self.client, stock_code_array=[code]
        )
        secu_abbr = self.get_juyuan_codeinfo(code[2:])[1]
        for one in rank.row:
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
                    raise
                if lead:
                    if stats in (3, 4):
                        return "{}竞价涨停, ".format(secu_abbr)
                    else:
                        return "{}竞价大涨{}%, ".format(secu_abbr, self.re_decimal_data(value))
                else:
                    return "{}跟涨{}%, ".format(secu_abbr, self.re_decimal_data(value))

    # def _create_table(self):
    #     self._target_init()
    #     sql = '''
    #     CREATE TABLE IF NOT EXISTS `{}` (
    #       `id` int(11) NOT NULL AUTO_INCREMENT,
    #       `Date` datetime NOT NULL COMMENT '资讯发布时间',
    #       `Title` varchar(64) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL COMMENT '生成文章标题',
    #       `Content` text CHARACTER SET utf8 COLLATE utf8_bin COMMENT '生成文章正文',
    #       `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
    #       `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    #        PRIMARY KEY (`id`),
    #        UNIQUE KEY `un2` (`Date`)
    #     ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='开盘异动盘口';
    #     '''.format(self.target_table)
    #     self.target_client.insert(sql)
    #     self.target_client.end()

    def _create_table(self):
        """
        新闻类型：
        1:  三日连续净流入前10个股
        2:  连板股今日竞价表现
        3:  早盘主力十大净买个股
        4:  开盘异动盘口
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

    def start(self):
        # 判断是否交易日
        is_trading = self.is_trading_day(self.day)
        if not is_trading:
            logger.warning('非交易日')
            return

        # 建表
        self._create_table()

        all_block_stats_map = self.get_all_block_stats(self.day)
        print(pprint.pformat(all_block_stats_map))   # 非交易日无数据

        if not all_block_stats_map:
            logger.warning("未获取到交易日的 block 信息")
            return

        all_block_codes = list(all_block_stats_map.keys())
        print(all_block_codes)

        block_rise_map = self.get_block_rise_map(all_block_codes)
        print(pprint.pformat(block_rise_map))     # 在非交易日是有数据的
        rise_block_codes = list(block_rise_map.keys())
        print(rise_block_codes)

        final = self.get_content(all_block_stats_map, block_rise_map)
        print(pprint.pformat(final))

        if not final:
            return

        ret = self._save(self.target_client, final, self.target_table, ['Title', 'Date', 'Content', 'NewsType', 'NewsJson'])
        if ret:
            self.ding("开盘异动资讯生成:\n{}".format(pprint.pformat(final)))


def task():
    ou = OpenUnusual()
    ou.start()


if __name__ == "__main__":
    # task()
    schedule.every().day.at("09:36").do(task)

    while True:
        schedule.run_pending()
        time.sleep(10)


'''说明文档 
开盘异动盘口
条件：取主题猎手-盘口异动9：36分时，出现涨停个股且涨幅大于1.5%的异动盘口，再取两个涨幅最高的跟涨个股

标题：
今日竞价表现：5G、半导体、券商板块活跃

内容：
今日竞价表现：
5G板块竞价涨幅达1.50%，华星创业竞价大涨9%，麦捷科技跟涨8%，有方科技跟涨7%。
半导体板块竞价涨幅达1.85%，通富微电竞价大涨8%，麦捷科技跟涨4%，有方科技跟涨5%。
券商板块竞价涨幅达2.57%，南京证券竞价涨停，国泰证券跟涨9%，中信证券跟涨8%。
'''

'''进入到根目录下进行部署 
docker build -f DockerfileUseApi2p -t registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/newsgenerator:v2 .
docker push registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/newsgenerator:v2 
sudo docker pull registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/newsgenerator:v2

sudo docker run --log-opt max-size=10m --log-opt max-file=3 -itd \
--env LOCAL=0 \
--name generate_openunusual \
registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/newsgenerator:v2 \
python Funds/open_unusual.py
'''