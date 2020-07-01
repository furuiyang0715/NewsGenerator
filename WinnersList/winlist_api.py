import datetime
import json
import os
import sys
import time
import traceback

import requests
import schedule

cur_path = os.path.split(os.path.realpath(__file__))[0]
file_path = os.path.abspath(os.path.join(cur_path, ".."))
sys.path.insert(0, file_path)

from base import NewsBase, logger


class OraApi(NewsBase):
    """龙虎榜接口数据"""
    def __init__(self):
        super(OraApi, self).__init__()
        self.url = 'http://bg.jingzhuan.cn?api=stock_rank_INS_Up&timeperiod=1'
        self.idx_table = 'stk_quot_idx'
        self.day = datetime.datetime.combine(datetime.datetime.today(), datetime.time.min)
        # self.day = datetime.datetime(2020, 6, 30)
        self.target_table = 'news_generate'
        self.fields = ['Title', 'Date', 'Content', 'NewsType', 'NewsJson']

    def _create_table(self):
        """
        新闻类型：
        1:  三日连续净流入前10个股
        2:  连板股今日竞价表现
        3:  早盘主力十大净买个股
        4:  开盘异动盘口
        5:  机构首次评级
        6:  获多机构买入增持评级
        7:  龙虎榜-机构净买额最大
        8:  龙虎榜-机构席位最多
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

    def get_changepercactual(self, inner_code):
        """获取行情数据"""
        self._dc_init()
        sql = '''select Date, Close, ChangePercActual from {} where InnerCode = '{}' and Date <= '{}' order by Date desc limit 1;
        '''.format(self.idx_table, inner_code, self.day)  # 因为假如今天被机构首次评级立即发布,未收盘前拿到的是昨天的行情数据, 收盘手拿到的是今天的
        ret = self.dc_client.select_one(sql)
        changepercactual = self.re_decimal_data(ret.get("ChangePercActual"))
        _close = self.re_decimal_data(ret.get("Close"))
        _date = ret.get("Date")    # 最近的一个已结束交易日的时间
        # TODO
        assert _date == self.day
        return changepercactual, _close

    def get_resp_datas(self):
        resp = requests.get(self.url)
        if resp.status_code == 200:
            body = resp.text
            datas = json.loads(body).get("response")
            return datas
        else:
            print(resp)
            return None

    def sort_datas(self, datas):
        # 找出机构净买额最大的一个
        datas = sorted(datas, key=lambda x: float(x["SUMNETBUY"]), reverse=True)
        data1 = datas[0]

        # 找出机构席位最多的一个
        datas = sorted(datas, key=lambda x: int(x['INSUP']), reverse=True)
        data2 = datas[0]

        return data1, data2

    def gene_content_maxnetbuy(self, data):
        inner_code, secu_abbr = self.get_juyuan_codeinfo(data['TRD_CODE'])

        title = '今日机构净买额最多个股为{}，机构净买额达{}，总净买额达{}。'.format(secu_abbr,
                                                          self.re_money_data(float(data['SUMNETBUY'])),
                                                          self.re_money_data(float(data['SUMBUY'])))

        changepercactual, _close = self.get_changepercactual(inner_code)
        rise_str = "涨幅" if changepercactual > 0 else "跌幅"
        content = '截至今日收盘，{}机构净买额{}，总净买金额达{}，今日收盘价{}，{}{}%。'.format(
            secu_abbr, self.re_money_data(float(data['SUMNETBUY'])),
            self.re_money_data(float(data['SUMBUY'])),
            _close, rise_str, changepercactual,
        )

        item = dict()
        item["Date"] = self.day
        item['Title'] = title
        item['Content'] = content
        item['NewsType'] = 7
        return item

    def gene_content_maxinsup(self, data):
        inner_code, secu_abbr = self.get_juyuan_codeinfo(data['TRD_CODE'])

        title = '龙虎榜今日机构买入席位最多个股为{}'.format(secu_abbr)

        changepercactual, _close = self.get_changepercactual(inner_code)
        rise_str = "涨幅" if changepercactual > 0 else "跌幅"
        content = '今日龙虎榜机构买席最多个股为{}，{}个机构买入，{}个机构卖出，购买金额为{}。今日收盘价{}，{}{}%。'.format(
            secu_abbr, data['INSBUY'], data['INSSELL'], self.re_money_data(float(data['SUMNETBUY'])),
            _close, rise_str, changepercactual,
        )

        item = dict()
        item["Date"] = self.day
        item['Title'] = title
        item['Content'] = content
        item['NewsType'] = 8
        return item

    def start(self):
        is_trading = self.is_trading_day(self.day)
        if not is_trading:
            logger.warning("非交易日")
            return

        datas = self.get_resp_datas()

        if not datas:
            logger.warning("接口异常, 请检查")
            return

        data1, data2 = self.sort_datas(datas)

        item1 = self.gene_content_maxnetbuy(data1)

        item2 = self.gene_content_maxinsup(data2)
        print(item1)
        print(item2)

        self._target_init()
        self._save(self.target_client, item1, self.target_table, self.fields)
        self._save(self.target_client, item2, self.target_table, self.fields)

        # TODO
        self.ding("龙虎榜-机构净买额最大: \n {}\n\n 龙虎榜-机构席位最多: \n{}".format(item1, item2))


def task():
    try:
        OraApi().start()
    except Exception:
        OraApi().ding("龙虎榜资讯生成异常:{} 请检查".format(traceback.format_exc()))


if __name__ == "__main__":
    # TODO
    # task()

    schedule.every().day.at("15:06").do(task)

    schedule.every().day.at("18:06").do(task)

    while True:
        schedule.run_pending()
        time.sleep(10)


'''
docker build -f DockerfileUseApi2p -t registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/newsgenerator:v2 .
docker push registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/newsgenerator:v2 
sudo docker pull registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/newsgenerator:v2

sudo docker run --log-opt max-size=10m --log-opt max-file=3 -itd \
--name generate_winlist \
registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/newsgenerator:v2 \
python WinnersList/winlist_api.py
'''



'''
龙虎榜-机构净买额最大
条件：龙虎榜机构净买额最大个股，收盘时发布
标题：今日机构净买额最多个股为宁德时代，机构净买额达10.04亿，总净买额达12.07亿。
内容：截至今日收盘，宁德时代机构净买额10.04亿，总净买金额达12.07亿，今日收盘价128.21，涨幅/跌幅+3.12%。

龙虎榜-机构席位最多
条件：龙虎榜机构买席数量最多个股，收盘时发布
标题：龙虎榜今日机构买入席位最多个股为山河药辅
内容：今日龙虎榜机构买席最多个股为山河药辅，10个机构买入，6个机构卖出，购买金额为10.12亿。今日收盘价128.21，涨幅/跌幅+3.12%。
'''


'''
 'response': [{
'END_DT': '2020-06-24 00:00:00', # 时间
'INSBUY': '4',                   # 买入结构个数 
'INSSELL': '2',                  # 卖出机构个数 
'INSUP': '6',                    # INSUP 机构数量=INSBUY+ISSELL 
'PREID': '5.48471658',            # 异动值 
'SUMBUY': '32152878.0000',        # 总买入 
'SUMNETBUY': '20672612.0000',     # 净买入 
'SUMSELL': '11480266.0000',       # 总卖出
'TRD_CODE': '002977',             # 股票代码 
'userdata': '002977',             # 同上 股票代码 
},
'''
