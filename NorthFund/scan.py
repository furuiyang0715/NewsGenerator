import datetime
import os
import sys
import time

cur_path = os.path.split(os.path.realpath(__file__))[0]
file_path = os.path.abspath(os.path.join(cur_path, ".."))
sys.path.insert(0, file_path)
from base import NewsBase, logger


class NorthFund(NewsBase):
    def __init__(self):
        super(NorthFund, self).__init__()
        self.source_table = 'hkland_flow'
        self.target_table = 'news_generate_flownorth'
        self.fields = ['Date', 'Threshold', 'Title', "Content", "DateTime", 'ShHkFlow', 'SzHkFlow']
        # 阈值 50亿  80亿  120亿
        self.thre1 = 50*10**4
        self.thre2 = 80*10**4
        self.thre3 = 120*10**4

    def _create_table(self):
        client = self._init_pool(self.product_cfg)
        sql = '''
        CREATE TABLE IF NOT EXISTS `{}` (
          `id` int(11) NOT NULL AUTO_INCREMENT,
          `Date` datetime NOT NULL COMMENT '日期', 
          `Threshold` decimal(19,0) NOT NULL COMMENT '阈值(单位:万)', 
          `DateTime` datetime NOT NULL COMMENT '达到阈值的分钟时间点', 
          `ShHkFlow` decimal(19,4) NOT NULL COMMENT '沪股通/港股通(沪)当日资金流向(万）', 
          `SzHkFlow` decimal(19,4) NOT NULL COMMENT '深股通/港股通(深)当日资金流向(万）',
          `Title` varchar(64) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL COMMENT '生成文章标题', 
          `Content` text CHARACTER SET utf8 COLLATE utf8_bin COMMENT '生成文章正文',           
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
           PRIMARY KEY (`id`),
           UNIQUE KEY `dt_thre` (`Date`, `Threshold`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='北向资金流入资讯生成';
        '''.format(self.target_table)
        client.insert(sql)
        client.dispose()

    def re_data2str(self, data):
        data = abs(data)
        # 将万转换为亿
        data, unit = ((data / 10 ** 4), "亿") if (data / 10 ** 4) > 1 else (data, "万")
        data = float("%.2f" % data)
        return "{}{}".format(data, unit)

    def fetch_data(self, start_dt, end_dt):
        client = self._init_pool(self.dc_cfg)
        positive = {}
        negative = {}
        for data in (self.thre1, self.thre2, self.thre3):
            sql = '''select DateTime,Netinflow,ShHkFlow,SzHkFlow from {} \
            where Category=2 and DateTime>='{}' and DateTime<='{}'\
            and Netinflow >= {} order by Datetime asc limit 1;
            '''.format(self.source_table, start_dt, end_dt, data)
            info = client.select_one(sql)
            positive[data] = info

        for data in ((-1)*self.thre1, (-1)*self.thre2, (-1)*self.thre3):
            sql = '''select DateTime,Netinflow,ShHkFlow,SzHkFlow from {} \
            where Category=2 and DateTime>='{}' and DateTime<='{}'\
            and Netinflow <= {} order by Datetime asc limit 1;
            '''.format(self.source_table, start_dt, end_dt, data)
            info = client.select_one(sql)
            negative[data] = info

        client.dispose()
        return positive, negative

    def produce(self, key, item):
        netinflow = item.get("Netinflow")
        shhkflow = item.get("ShHkFlow")
        szhkflow = item.get("SzHkFlow")
        date_time = item.get("DateTime")
        flag_str_netin = "流入" if netinflow > 0 else "流出"
        flag_str_shhk = "流入" if shhkflow > 0 else "流出"
        flag_str_szhk = "流入" if szhkflow > 0 else "流出"
        netinflow_info = self.re_data2str(abs(netinflow))
        shhkflow_info = self.re_data2str(abs(shhkflow))
        szhkflow_info = self.re_data2str(abs(szhkflow))
        title = "截止目前北向资金当前净" + flag_str_netin + "{}元".format(netinflow_info)
        content = "截至{}月{}日{}时{}分，北向资金净{}{}元，其中沪股通{}{}元，深股通{}{}元。".format(
            date_time.month, date_time.day, date_time.hour, date_time.minute,
            flag_str_netin, netinflow_info,
            flag_str_shhk, shhkflow_info, flag_str_szhk, szhkflow_info
        )

        item = dict()
        item['Date'] = datetime.datetime.combine(date_time, datetime.time.min)
        item['Threshold'] = key
        item['Title'] = title
        item['Content'] = content
        item['DateTime'] = date_time
        item['ShHkFlow'] = shhkflow
        item['SzHkFlow'] = szhkflow
        return item

    def get_final_data(self, end_dt):
        client = self._init_pool(self.dc_cfg)
        sql = '''select DateTime,Netinflow,ShHkFlow,SzHkFlow from {} \
        where Category=2 and DateTime='{}';
        '''.format(self.source_table, end_dt)
        final_data = client.select_one(sql)
        client.dispose()
        if not final_data:
            return None

        netinflow = final_data.get("Netinflow")
        shhkflow = final_data.get("ShHkFlow")
        szhkflow = final_data.get("SzHkFlow")
        date_time = final_data.get("DateTime")
        flag_str_netin = "流入" if netinflow > 0 else "流出"
        flag_str_shhk = "流入" if shhkflow > 0 else "流出"
        flag_str_szhk = "流入" if szhkflow > 0 else "流出"
        netinflow_info = self.re_data2str(abs(netinflow))
        shhkflow_info = self.re_data2str(abs(shhkflow))
        szhkflow_info = self.re_data2str(abs(szhkflow))
        title = "截至今日收盘,北向资金净" + flag_str_netin + "{}元".format(netinflow_info)
        content = "截至{}月{}日，北向资金净{}{}元，其中沪股通{}{}元，深股通{}{}元。".format(
            date_time.month, date_time.day,
            flag_str_netin, netinflow_info,
            flag_str_shhk, shhkflow_info,
            flag_str_szhk, szhkflow_info
        )
        item = dict()
        item['Date'] = datetime.datetime.combine(end_dt, datetime.time.min)
        item['Threshold'] = 0
        item['Title'] = title
        item['Content'] = content
        item['DateTime'] = date_time
        item['ShHkFlow'] = shhkflow
        item['SzHkFlow'] = szhkflow
        return item

    def start(self, _now):
        self._create_table()
        _start = datetime.datetime(_now.year, _now.month, _now.day, 9, 30, 0)
        _end = datetime.datetime(_now.year, _now.month, _now.day, 15, 0, 0)
        client = self._init_pool(self.product_cfg)
        if _now > _end:
            # 入库北向资金当日收盘流入
            item = self.get_final_data(_end)
            if item:
                self._save(client, item, self.target_table, self.fields)

        positive, negative = self.fetch_data(_start, _now)
        positive.update(negative)
        for key, value in positive.items():
            if value:
                item = self.produce(key, value)
                self._save(client, item, self.target_table, self.fields)
        client.dispose()

    def history(self):
        client = self._init_pool(self.product_cfg)
        s_day = datetime.datetime(2020, 1, 1)
        e_day = datetime.datetime(2020, 5, 26)
        _day = s_day
        while _day <= e_day:

            positive, negative = self.fetch_data(datetime.datetime(_day.year, _day.month, _day.day, 9, 30),
                                                 datetime.datetime(_day.year, _day.month, _day.day, 15, 0))
            positive.update(negative)
            for key, value in positive.items():
                if value:
                    item = self.produce(key, value)
                    self._save(client, item, self.target_table, self.fields)

            item = self.get_final_data(datetime.datetime(_day.year, _day.month, _day.day, 15, 0))
            if item:
                self._save(client, item, self.target_table, self.fields)

            _day += datetime.timedelta(days=1)
            print()
            print()


if __name__ == "__main__":
    while True:
        north = NorthFund()
        _now = datetime.datetime.now()
        day_start = datetime.datetime(_now.year, _now.month, _now.day, 9, 25, 0)
        day_end = datetime.datetime(_now.year, _now.month, _now.day, 15, 5, 0)
        if _now <= day_start or _now >= day_end:
            logger.info("不在生成时间内 {}".format(_now))
            time.sleep(30)
        else:
            north.start(_now)
            time.sleep(10)

