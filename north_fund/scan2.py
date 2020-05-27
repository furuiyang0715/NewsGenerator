import datetime

from Finance.base import NewsBase


class NorthFund(NewsBase):
    def __init__(self):
        super(NorthFund, self).__init__()
        self.source_table = 'hkland_flow'
        self.target_table = 'news_generate_flownorth'
        self.fields = ['Date', 'Threshold', 'Title', "Content", "DateTime"]
        self.day = datetime.datetime.combine(datetime.datetime.today(), datetime.time.min)
        self.thre1 = 1*10**4
        self.thre2 = 2*10**4
        self.thre3 = 3*10**4

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
        item['Date'] = self.day
        item['Threshold'] = key
        item['Title'] = title
        item['Content'] = content
        item['DateTime'] = date_time
        return item

    def start(self):
        # (1) 获取当前的时间点
        _now = datetime.datetime.now()
        # (2) 从当天的开始时间(或者北向资金的开始时间 9 点半)到当前时间之间的全部数据中最大的一只
        _start = datetime.datetime(_now.year, _now.month, _now.day, 9, 30, 0)
        positive, negative = self.fetch_data(_start, _now)
        positive.update(negative)
        client = self._init_pool(self.product_cfg)
        for key, value in positive.items():
            if value:
                item = self.produce(key, value)
                print(item)
                # self._save(client, item, self.target_table, self.fields)
        client.dispose()


if __name__ == "__main__":
    north = NorthFund()
    north.start()
