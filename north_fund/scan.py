import datetime

from Finance.base import NewsBase, logger


class NorthFund(NewsBase):
    def __init__(self):
        super(NorthFund, self).__init__()
        self.source_table = 'hkland_flow'
        self.target_table = 'news_generate_flownorth'
        self.fields = ['Title', "Content", "DateTime", 'Threshold']

    def fetch_data(self, start_dt, end_dt):
        sql = '''select DateTime,Netinflow,ShHkFlow,SzHkFlow from {} \
        where Category=2 and DateTime>='{}' and DateTime<='{}'\
        and Netinflow=(select max(Netinflow) from {} where Category=2 and DateTime>='{}' and DateTime<='{}');
        '''.format(self.source_table, start_dt, end_dt, self.source_table, start_dt, end_dt)
        client = self._init_pool(self.dc_cfg)
        ret = client.select_one(sql)
        client.dispose()
        return ret

    def _create_table(self):
        client = self._init_pool(self.product_cfg)
        sql = '''
        CREATE TABLE IF NOT EXISTS `{}` (
          `id` int(11) NOT NULL AUTO_INCREMENT,
          `Title` varchar(64) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL COMMENT '生成文章标题', 
          `Content` text CHARACTER SET utf8 COLLATE utf8_bin COMMENT '生成文章正文',
          `DateTime` datetime NOT NULL COMMENT '达到阈值的分钟时间点',
          `Positive` int NOT NULL COMMENT '北向资金流入流出标志: 1流入0流出 ',          
          `Threshold` int NOT NULL COMMENT '阈值(单位:亿)',          
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
           PRIMARY KEY (`id`),
           UNIQUE KEY `date_time` (`DateTime`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='北向资金流入资讯生成';
        '''.format(self.target_table)
        client.insert(sql)
        client.dispose()

    def start(self):
        # 建表
        self._create_table()

        # (1) 获取当前的时间点
        _now = datetime.datetime.now()
        # (2) 从当天的开始时间(或者北向资金的开始时间 9 点半)到当前时间之间的全部数据中最大的一只
        _start = datetime.datetime(_now.year, _now.month, _now.day, 9, 30, 0)
        data = self.fetch_data(_start, _now)

        netinflow = data.get("Netinflow")
        shhkflow = data.get("ShHkFlow")
        szhkflow = data.get("SzHkFlow")
        date_time = data.get("DateTime")

        flag_str_netin = "流入" if netinflow > 0 else "流出"
        flag_str_shhk = "流入" if shhkflow > 0 else "流出"
        flag_str_szhk = "流入" if szhkflow > 0 else "流出"

        positive = 1 if netinflow > 0 else 0

        netinflow = abs(netinflow)
        shhkflow = abs(shhkflow)
        szhkflow = abs(szhkflow)

        if netinflow >= 120 * 10 ** 4:     # 120 亿
            _threshold = 120
        elif netinflow >= 80 * 10 ** 4:    # 80 亿
            _threshold = 80
        elif netinflow >= 50 * 10 ** 4:     # 50 亿
            _threshold = 50
        else:
            logger.info("{} 未达到最近触发阈值".format(netinflow))
            return

        netinflow_info = self.re_data2str(netinflow)
        shhkflow_info = self.re_data2str(shhkflow)
        szhkflow_info = self.re_data2str(szhkflow)

        title = "截止目前北向资金当前净" + flag_str_netin + "{}元".format(netinflow_info)
        content = "截至{}月{}日{}时{}分，北向资金净{}{}元，其中沪股通{}{}元，深股通{}{}元。".format(
            date_time.month, date_time.day, date_time.hour, date_time.minute,
            flag_str_netin, netinflow_info,
            flag_str_shhk, shhkflow_info, flag_str_szhk, szhkflow_info
        )

        # 获取数据库中已有的最大阈值 根据当前是流入和流出进行对应判断
        client = self._init_pool(self.product_cfg)
        sql = '''select max(Threshold) as max_threshold from {} where DateTime >= '{}' and Positive = {}; '''.format(
            self.target_table, _start, positive)
        ret = client.select_one(sql)
        max_threshold = ret.get("max_threshold")
        if not max_threshold or max_threshold < _threshold:
            item = dict()
            item['Title'] = title
            item['Content'] = content
            item['DateTime'] = date_time
            item['Positive'] = positive
            item['Threshold'] = _threshold  # 单位: 亿
            self._save(client, item, self.target_table, self.fields)
        else:
            logger.info("非首次突破阈值不生成")

    def re_data2str(self, data):
        data = abs(data)
        # 将万转换为亿
        data, unit = ((data / 10 ** 4), "亿") if (data / 10 ** 4) > 1 else (data, "万")
        data = float("%.2f" % data)
        return "{}{}".format(data, unit)


if __name__ == "__main__":
    north = NorthFund()
    north.start()

    # # 测试数据转换
    # d1 = 500
    # print(north.re_data2str(d1))
    #
    # d2 = 5678.8899
    # print(north.re_data2str(d2))
    #
    # d3 = 4500008.3736
    # print(north.re_data2str(d3))
