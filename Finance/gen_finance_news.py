import datetime
import pprint

from Finance.base import NewsBase


class GenFiance(NewsBase):
    def __init__(self, company_code,
                 # secu_code, secu_abbr,
                 ):
        super(GenFiance, self).__init__()
        self.company_code = company_code
        # self.secu_code = secu_code
        # self.secu_addr = secu_abbr

        self.source_table = 'LC_IncomeStatementAll'

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
        print(ret_this)
        print(ret_last)

        # 计算触发条件
        netprofit_this, netprofit_last = ret_this.get("NetProfit"), ret_last.get("NetProfit")
        threshold = (netprofit_this - netprofit_last) / netprofit_last

        if netprofit_this > 0 and netprofit_last > 0:
            if threshold >= 0.5:
                self.inc_50(ret_this, ret_last)
            elif 0 < threshold < 0.5:
                self.inc(ret_this, ret_last)
            elif threshold < 0:
                self.reduce(ret_this, ret_last)

        elif netprofit_this < 0 and netprofit_last > 0:
            self.gain_to_loss(ret_this, ret_last)
        elif netprofit_this > 0 and netprofit_last < 0:
            self.loss_to_gain(ret_this, ret_last)
        elif netprofit_this < 0 and netprofit_last < 0 and abs(netprofit_this) < abs(netprofit_last):
            self.ease_loss(ret_this, ret_last)
        elif netprofit_this < 0 and netprofit_last < 0 and abs(netprofit_this) > abs(netprofit_last):
            if threshold > 0.5:
                self.intensify_loss_50(ret_this, ret_last)
            else:
                self.intensify_loss(ret_this, ret_last)

    def inc_50(self, ret_this, ret_last):
        """大幅盈增"""
        pass
        # 大幅增盈的触发条件: 比去年的同期盈利增大 50% 以上; 当日发布季度报告 --> 生成一条新闻
        # item = dict()
        # item['EndDate'] = ret_this.get("EndDate")     # 最新一季的时间
        # item['InfoPublDate'] = ret_this.get("InfoPublDate")   # 最新一季报表的发布时间
        # item['CompanyCode'] = self.company_code
        # item['SecuCode'] = self.secu_code
        # item['SecuAbbr'] = self.secu_addr
        #
        #
        # pass

    def inc(self, ret_this, ret_last):
        """增盈"""
        pass

    def reduce(self, ret_this, ret_last):
        """减盈"""
        pass

    def gain_to_loss(self, ret_this, ret_last):
        """由盈转亏"""
        pass

    def loss_to_gain(self, ret_this, ret_last):
        """由亏转盈"""
        pass

    def ease_loss(self, ret_this, ret_last):
        """减亏"""
        pass

    def intensify_loss_50(self, ret_this, ret_last):
        """大幅增亏"""
        pass

    def intensify_loss(self, ret_this, ret_last):
        """增亏"""
        pass


if __name__ == "__main__":
    g = GenFiance(3)
    # ret = g.get_quarter_info(datetime.datetime(2020, 3, 31))
    # print(pprint.pformat(ret))

    g.start()
