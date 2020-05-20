import datetime
import pprint
import sys

from Finance.base import NewsBase, logger


class GenFiance(NewsBase):
    def __init__(self, company_code,
                 secu_code, secu_abbr,
                 ):
        super(GenFiance, self).__init__()
        self.company_code = company_code
        self.secu_code = secu_code
        self.secu_addr = secu_abbr

        self.source_table = 'LC_IncomeStatementAll'

        self.quarter_map = {
            3: "一",
            6: "二",
            9: "三",
            12: "四",
        }

    def get_quarter_info(self, quarter: datetime.datetime):
        juyuan = self._init_pool(self.juyuan_cfg)
        sql = '''
select InfoPublDate, EndDate, IfMerged, IfAdjusted, NetProfit, OperatingRevenue, BasicEPS \
from {} where CompanyCode={} and IfMerged=1 \
and NetProfit is not NULL and OperatingRevenue is not null and BasicEPS is not null \
and EndDate = '{}' and IfAdjusted in (1,2) \
ORDER BY InfoPublDate desc, IfAdjusted asc limit 1;
        '''.format(self.source_table, self.company_code, quarter)
        # 升序为 asc 降序为 desc
        # print(sql)
        ret = juyuan.select_one(sql)
        return ret

    def start(self):
        # TODO 季度节点的获取逻辑 文档中有一句 "当日发表季报文档", 以下途径仅为暂时测试使用
        _quarter_this = datetime.datetime(2020, 3, 31)
        _quarter_last = datetime.datetime(2019, 3, 31)

        ret_this, ret_last = self.get_quarter_info(_quarter_this), self.get_quarter_info(_quarter_last)
        logger.info("本期: \n{}\n".format(pprint.pformat(ret_this)))
        logger.info("上期: \n{}\n".format(pprint.pformat(ret_last)))

        # 计算营业额的阈值
        operatingrevenue_this, operatingrevenue_last = ret_this.get("OperatingRevenue"), ret_last.get("OperatingRevenue")
        r_threshold = (operatingrevenue_this - operatingrevenue_last) / operatingrevenue_last
        logger.info("营业额同比计算值: {}".format(r_threshold))

        # 计算触发条件 净利润的阈值
        netprofit_this, netprofit_last = ret_this.get("NetProfit"), ret_last.get("NetProfit")
        threshold = (netprofit_this - netprofit_last) / netprofit_last
        logger.info("净利润同比计算值: {}".format(threshold))

        if netprofit_this > 0 and netprofit_last > 0:
            if threshold >= 0.5:
                self.inc_50(ret_this, ret_last, threshold, r_threshold)
            elif 0 < threshold < 0.5:
                self.inc(ret_this, ret_last, threshold, r_threshold)
            elif threshold < 0:
                self.reduce(ret_this, ret_last, threshold, r_threshold)

        elif netprofit_this < 0 and netprofit_last > 0:
            self.gain_to_loss(ret_this, ret_last, threshold, r_threshold)
        elif netprofit_this > 0 and netprofit_last < 0:
            self.loss_to_gain(ret_this, ret_last, threshold, r_threshold)
        elif netprofit_this < 0 and netprofit_last < 0 and abs(netprofit_this) < abs(netprofit_last):
            self.ease_loss(ret_this, ret_last, threshold, r_threshold)
        elif netprofit_this < 0 and netprofit_last < 0 and abs(netprofit_this) > abs(netprofit_last):
            if threshold > 0.5:
                self.intensify_loss_50(ret_this, ret_last, threshold, r_threshold)
            else:
                self.intensify_loss(ret_this, ret_last, threshold, r_threshold)

    def re_percent_data(self, data):
        """处理百分率数据"""
        # ret = float("%.4f" % data) * 100
        ret = float("%.2f" % (data * 100))
        return ret

    def re_hundredmillion_data(self, data):
        """将元转换为亿元 并且保留两位小数"""
        ret = float("%.2f" % (data / 10**8))
        return ret

    def re_ten_thousand_data(self, data):
        """将元转换为万元 并且保留两位小数"""
        ret = float("%.2f" % (data / 10**4))
        return ret

    def re_decimal_data(self, data):
        """一般小数保留里两位"""
        ret = float("%.2f" % data)
        return ret

    def re_money_data(self, data):
        """根据元数量的大小将其转换为对应的万元、亿元等
        """
        data = abs(data)
        if 0 <= data < 10 ** 8:   # 小于 1 亿的钱以万为单位
            data = self.re_ten_thousand_data(data)
            return "{}万".format(data)
        else:
            data = self.re_hundredmillion_data(data)
            return "{}亿".format(data)

    def _process_data(self, ret_this, ret_last, threshold, r_threshold, title_format, content_format, change_type):
        this_end_date = ret_this.get("EndDate")

        # 转换为 保留两位的百分数
        threshold = self.re_percent_data(threshold)
        r_threshold = self.re_percent_data(r_threshold)

        # 营业收入的单位从 元 转换为 亿元
        this_operating_revenue = self.re_money_data(ret_this.get("OperatingRevenue"))
        last_operating_revenue = self.re_money_data(ret_last.get("OperatingRevenue"))

        # 净利润的单位 从 元 转换 为 万元
        this_net_profit = self.re_money_data(ret_this.get("NetProfit"))
        last_net_profit = self.re_money_data(ret_last.get("NetProfit"))

        this_basic_EPS = self.re_decimal_data(ret_this.get("BasicEPS"))
        last_basic_EPS = self.re_decimal_data(ret_last.get("BasicEPS"))

        quarter_info = """{}年第{}季度""".format(this_end_date.year, self.quarter_map.get(this_end_date.month))
        item = dict()
        item['EndDate'] = this_end_date  # 最新一季的时间
        item['InfoPublDate'] = ret_this.get("InfoPublDate")  # 最新一季报表的发布时间
        item['CompanyCode'] = self.company_code
        item['SecuCode'] = self.secu_code
        item['SecuAbbr'] = self.secu_addr
        item['ChangeType'] = change_type
        # 指标参数也保留在生成数据库中
        item['NetProfit'] = ret_this.get("NetProfit")
        title = title_format.format(self.secu_addr, quarter_info, this_net_profit, threshold)
        item['title'] = title
        content = content_format.format(self.secu_addr, quarter_info, self.secu_addr, quarter_info,
                                        this_operating_revenue, r_threshold,
                                        this_net_profit, threshold,
                                        this_basic_EPS,
                                        last_net_profit, last_basic_EPS)
        item['content'] = content
        logger.info(pprint.pformat(item))

    def inc_50(self, ret_this, ret_last, threshold, r_threshold):
        """大幅盈增
        触发条件: 比去年的同期盈利增大 50% 以上; 当日发布季度报告 --> 生成一条新闻
        """
        logger.info("大幅盈增")
        title_format = """大幅增盈-{}{}净利{}，同比增长{}%"""
        content_format = '''大幅增盈-{}{}业绩: {}{}实现营业收入{}, 同期增长{}%, 净利润{}元, 同期增长{}%。基本每股收益{}元, 上年同期业绩净利润{}元, 基本每股收益{}元。'''
        change_type = 1
        self._process_data(ret_this, ret_last, threshold, r_threshold, title_format, content_format, change_type)

    def inc(self, ret_this, ret_last, threshold, r_threshold):
        """增盈"""
        logger.info("增盈")
        title_format = """增盈-{}{}净利{}，同比增长{}%"""
        content_format = '''增盈-{}{}业绩: {}{}实现营业收入{}, 同期增长{}%, 净利润{}元, 同期增长{}%。基本每股收益{}元, 上年同期业绩净利润{}元, 基本每股收益{}元。'''
        change_type = 1
        self._process_data(ret_this, ret_last, threshold, r_threshold, title_format, content_format, change_type)

    def reduce(self, ret_this, ret_last, threshold, r_threshold):
        """减盈"""
        logger.info("减盈")
        title_format = '减盈-{}{}净利{}，同比下跌{}%'


    def gain_to_loss(self, ret_this, ret_last, threshold, r_threshold):
        """由盈转亏"""
        logger.info("由盈转亏")

    def loss_to_gain(self, ret_this, ret_last, threshold, r_threshold):
        """由亏转盈"""
        logger.info("由亏转盈")

    def ease_loss(self, ret_this, ret_last, threshold, r_threshold):
        """减亏"""
        logger.info("减亏")

    def intensify_loss_50(self, ret_this, ret_last, threshold, r_threshold):
        """大幅增亏"""
        logger.info("大幅增亏")

    def intensify_loss(self, ret_this, ret_last, threshold, r_threshold):
        """增亏"""
        logger.info("增亏")


if __name__ == "__main__":
    g = GenFiance(3, '000001', '平安银行')
    g.start()
