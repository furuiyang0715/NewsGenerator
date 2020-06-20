import datetime
import os
import sys
import time

import schedule

cur_path = os.path.split(os.path.realpath(__file__))[0]
file_path = os.path.abspath(os.path.join(cur_path, ".."))
sys.path.insert(0, file_path)
from base import NewsBase, logger


class OrganizationEvaluation(NewsBase):
    """机构首次生成 昨日数据汇总"""
    def __init__(self, _today=None):
        super(OrganizationEvaluation, self).__init__()
        self.source_table = 'rrp_rpt_secu_rat'
        self.idx_table = 'stk_quot_idx'
        self.target_table = 'news_generate_organization'
        # 需求是在过 0 点的时候生成前一天的整合新闻
        self._today = datetime.datetime.combine(datetime.datetime.today(), datetime.time.min) if not _today else _today
        self.day = self._today - datetime.timedelta(days=1)
        self.bg_client = None
        self.dc_client = None
        self.target_client = None
        self.title_format = '{}月{}日{}只个股获机构首次评级: '
        self.content_format = '{}（{}）获得{}首次评级 - {}，最新收盘价{}，涨幅{}%。'

    def __del__(self):
        if self.bg_client:
            self.bg_client.dispose()
        if self.dc_client:
            self.dc_client.dispose()
        if self.target_client:
            self.target_client.dispose()

    def _bg_init(self):
        if self.bg_client:
            return
        self.bg_client = self._init_pool(self.bigdata_cfg)

    def _dc_init(self):
        if self.dc_client:
            return
        self.dc_client = self._init_pool(self.dc_cfg)

    def _target_init(self):
        if self.target_client:
            return
        self.target_client = self._init_pool(self.product_cfg)

    def get_pub_first(self):
        """昨日个股获得新机构首次评级，第二天9点40发布"""
        self._bg_init()
        select_fields = 'pub_dt,trd_code,secu_sht, com_id,com_name,rat_code,rat_desc'
        # 查询发布时间等于指定时间的全量数据
        sql = '''select {} from {} where pub_dt = '{}';'''.format(select_fields, self.source_table, self.day)
        ret = self.bg_client.select_all(sql)
        return ret

    def get_evaluate_more(self):
        """昨日获得五家（含）以上机构买入或获增持评级个股，第二天9点40发布"""
        self._bg_init()
        sql = '''select trd_code,count(*) as count from {} where pub_dt = '{}' \
and rat_code in (10, 20) group by trd_code having count(*) >=5;'''.format(self.source_table, self.day)
        ret = self.bg_client.select_all(sql)
        return ret

    def check_pub_first(self, data):
        """判断是否是机构首次发布"""
        sql = '''select com_id,trd_code,pub_dt from {} where com_id = '{}' and trd_code = '{}' and pub_dt < '{}'; 
        '''.format(self.source_table, data.get("com_id"), data.get("trd_code"), data.get("pub_dt"))
        ret = self.bg_client.select_all(sql)
        if not ret:
            return True
        else:
            return False

    def _create_table(self):
        sql = '''
        CREATE TABLE IF NOT EXISTS `{}` (
          `id` int(11) NOT NULL AUTO_INCREMENT,
          `PubDate` datetime NOT NULL COMMENT '资讯发布时间', 
          `PubType` int NOT NULL COMMENT '资讯类型1:首次评级2:获多机构买入增持评级',
          `Title` varchar(64) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL COMMENT '生成文章标题', 
          `Content` text CHARACTER SET utf8 COLLATE utf8_bin COMMENT '生成文章正文',
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
           PRIMARY KEY (`id`),
           UNIQUE KEY `un2` (`PubDate`, `PubType`) 
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='机构评级资讯生成';
        '''.format(self.target_table)
        self._target_init()
        self.target_client.insert(sql)
        logger.info("建表成功 ")

    def process_items(self, datas):
        secu_codes = self.a_secucategory_codes

        # 对首次发布的数据再次进行筛选
        # (1) A 股票
        # (2) 存在聚源内部编码
        # (3) 可查询到当日的收盘价以及涨跌幅
        items = []
        for data in datas:
            trd_code = data.get("trd_code")
            if not trd_code in secu_codes:
                logger.info("非A股")
                continue
            secu_sht = data.get("secu_sht")
            com_name = data.get("com_name")
            rat_desc = data.get("rat_desc")

            item = dict()
            if len(trd_code) < 6:
                trd_code = (6-len(trd_code))*"0" + trd_code
            item["SecuCode"] = trd_code
            item['SecuAbbr'] = secu_sht
            inner_code = self.get_inner_code_bysecu(trd_code)
            if not inner_code:
                continue

            sql = '''select Close, ChangePercActual from {} where InnerCode = {} and Date <= '{}' order by Date desc limit 1; 
            '''.format(self.idx_table, inner_code, self.day)
            ret = self.dc_client.select_one(sql)
            if not ret:
                logger.info("{} {} 无法查询到 {} 的收盘价以及涨跌幅".format(trd_code, secu_sht, self.day))
                continue

            _close = self.re_decimal_data(ret.get("Close"))
            changepercactual = self.re_decimal_data(ret.get("ChangePercActual"))
            content = self.content_format.format(secu_sht, trd_code, com_name, rat_desc, _close, changepercactual)
            item['Close'] = _close
            item['ChangePercActual'] = changepercactual
            item['Content'] = content
            logger.debug(item)
            items.append(item)

        if len(items) == 0:
            logger.warning("{} 无符合条件的首次发布数据".format(self.day))
            return

        title = self.title_format.format(self.day.month, self.day.day, len(items))
        content = ''
        for item in items:
            content += (item.get("Content") + "\n")

        ret = dict()
        ret["PubDate"] = self._today
        ret['PubType'] = 1
        ret['Title'] = title
        ret["Content"] = content
        self._save(self.target_client, ret, self.target_table, ["PubDate", 'PubType', 'Title', "Content"])

    def pub_first_news(self):
        """机构首次评审"""
        self._create_table()
        datas = self.get_pub_first()
        logger.info("{} 的发布个数为 {}".format(self.day, len(datas)))

        self._dc_init()
        first_datas = []
        for data in datas:
            is_first = self.check_pub_first(data)
            if is_first:
                first_datas.append(data)

        # first_datas-items 代表符合条件的数据
        if first_datas:
            logger.info("{} 首次发布的个数 {}".format(self.day, len(first_datas)))
            self.process_items(first_datas)
        else:
            logger.info("{} 无首次发布".format(self.day))

    def evaluate_more(self):
        secu_codes = self.a_secucategory_codes
        self._create_table()
        datas = self.get_evaluate_more()

        # more_datas 代表符合要求的数据
        # (1) A 股
        # (2) 可查询到聚源内部编码
        # (3) 可查询到当日的收盘价以及涨跌幅
        more_datas = []
        for data in datas:
            trd_code = data.get("trd_code")
            if not trd_code in secu_codes:
                logger.info("非 A 股")
                continue
            inner_code = self.get_inner_code_bysecu(trd_code)
            if not inner_code:
                continue
            sql = '''select Close, ChangePercActual from {} where InnerCode = {} and Date <= '{}' order by Date desc limit 1; 
                        '''.format(self.idx_table, inner_code, self.day)
            ret = self.dc_client.select_one(sql)
            if not ret:
                logger.info("{} 无法查询到 {} 的收盘价以及涨跌幅".format(trd_code, self.day))
                continue
            _close = self.re_decimal_data(ret.get("Close"))
            changepercactual = self.re_decimal_data(ret.get("ChangePercActual"))
            data['_close'] = _close
            data['changepercactual'] = changepercactual
            more_datas.append(data)

        if len(more_datas) == 0:
            logger.info("{} 今日无满足要求的多机构评级数据".format(self.day))
            return

        content = ''
        for data in more_datas:
            count = data.get("count")
            trd_code = data.get("trd_code")
            _close = data.get("_close")
            changepercactual = data.get("changepercactual")

            sql = """select pub_dt,trd_code,secu_sht, com_id,com_name,rat_code,rat_desc from {} \
            where pub_dt = '{}' and  rat_code in (10, 20) and trd_code = '{}';""".format(self.source_table, self.day, trd_code)
            ret = self.bg_client.select_one(sql)
            secu_sht = ret.get('secu_sht')
            rat_desc = ret.get("rat_desc")
            self._dc_init()
            content += '{}（{}）获{}家机构评级-{}，最新收盘价{}，涨幅{}%。 \n'.format(secu_sht, trd_code, count, rat_desc, _close, changepercactual)

        # title = '{}月{}日{}只个股获5家以上机构评级'.format(self.day.month, self.day.day, len(more_datas))
        # 修改标题: 6月1日23只个股获多家机构调高评级
        title = '{}月{}日{}只个股获多家机构调高评级'.format(self.day.month, self.day.day, len(more_datas))
        final = dict()
        final["PubDate"] = self._today
        final['PubType'] = 2
        final['Title'] = title
        final['Content'] = content
        self._save(self.target_client, final, self.target_table, ["PubDate", 'PubType', 'Title', 'Content'])


def task():
    """当日数据"""
    runner = OrganizationEvaluation()
    runner.pub_first_news()
    print()
    runner.evaluate_more()


def history_task():
    """历史数据"""
    _start = datetime.datetime(2020, 1, 13)
    _end = datetime.datetime(2020, 6, 3)
    _dt = _start
    while _dt <= _end:
        print(_dt)
        runner = OrganizationEvaluation(_dt)
        runner.pub_first_news()
        print()
        runner.evaluate_more()
        _dt += datetime.timedelta(days=1)
    print("END")


if __name__ == "__main__":
    history_task()
    task()
    schedule.every().day.at("00:05").do(task)
    schedule.every().day.at("09:00").do(task)

    while True:
        # print("当前调度系统中的任务列表 {}".format(schedule.jobs))
        schedule.run_pending()
        time.sleep(30)


'''进入到根目录下进行部署: 机构类汇总 首次评级和多机构评级同时生成 
docker build -f Dockerfile -t registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/newsgenerator:v1 . 
docker push registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/newsgenerator:v1
sudo docker pull registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/newsgenerator:v1 
sudo docker run --log-opt max-size=10m --log-opt max-file=3 -itd \
--env LOCAL=1 \
--name ora \
registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/newsgenerator:v1 \
python OrganizationEvaluation/huddle.py
'''
