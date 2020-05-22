import datetime
import pprint
import sys
from decimal import Decimal

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
        self.change_type_map = {
            1: "大幅增盈",
            2: "增盈",
            3: "减盈",
            4: "由盈转亏",
            5: "由亏转盈",
            6: "减亏",
            7: "增亏",
            8: '大幅增亏',
        }
        self.fields = [
            'EndDate',    # 最新一季的季度节点时间
            'InfoPublDate',  # 最新一季季度节点时间对应的发布时间
            'CompanyCode',   # 公司代码
            'SecuCode',  # 证券代码
            'SecuAbbr',  # 证券简称
            'ChangeType',  # 新闻类型(1大幅增盈, 2增盈, 3减盈, 4由盈转亏, 5由亏转盈, 6减亏, 7增亏, 8大幅增亏)
            'NPParentCompanyOwners',  # 母公司净利润
            'Title',  # 生成文章标题
            'Content',  # 生成文章正文
        ]

    def _create_table(self):
        client = self._init_pool(self.product_cfg)
        # 联合唯一主键： EndDate, CompanyCode
        sql = '''
        CREATE TABLE IF NOT EXISTS `news_generate_finance` (
          `id` int(11) NOT NULL AUTO_INCREMENT,
          `EndDate` datetime NOT NULL COMMENT '最近发布的截止日期',
          `InfoPublDate` datetime NOT NULL COMMENT '最近发布的信息发布日期',
          `CompanyCode` int(11) NOT NULL COMMENT '公司代码',
          `SecuCode` varchar(10) DEFAULT NULL COMMENT '证券代码', 
          `SecuAbbr` varchar(100) DEFAULT NULL COMMENT '证券简称',
          `ChangeType` int NOT NULL COMMENT '生成新闻类型(1大幅增盈, 2增盈, 3减盈, 4由盈转亏, 5由亏转盈, 6减亏, 7增亏, 8大幅增亏)', 
          `NPParentCompanyOwners` decimal(19,4) DEFAULT NULL COMMENT '归属于母公司所有者的净利润', 
          `Title` varchar(64) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL COMMENT '生成文章标题',
          `Content` text CHARACTER SET utf8 COLLATE utf8_bin COMMENT '生成文章正文',
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
           PRIMARY KEY (`id`),
           UNIQUE KEY `un2` (`EndDate`, `CompanyCode`) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='深交所融资融券标的证券历史清单';
        '''
        client.insert(sql)
        client.dispose()

    def get_quarter_info(self, quarter: datetime.datetime):
        juyuan = self._init_pool(self.juyuan_cfg)
        sql = '''
select InfoPublDate, EndDate, IfMerged, IfAdjusted, NPParentCompanyOwners, OperatingRevenue, BasicEPS \
from {} where CompanyCode={} and IfMerged=1 \
and NetProfit is not NULL and OperatingRevenue is not null and BasicEPS is not null \
and EndDate = '{}' and IfAdjusted in (1,2) \
ORDER BY InfoPublDate desc, IfAdjusted asc limit 1;
        '''.format(self.source_table, self.company_code, quarter)
        # 升序为 asc 降序为 desc
        logger.debug(sql)
        ret = juyuan.select_one(sql)
        return ret

    def diff_quarters(self, _quarter_this, _quarter_last):
        """获取两个季度的数据库信息 进行对比以及指标计算 """
        # 从数据库中获取到上一期的值 和 这一期的值, 均是原始数据
        ret_this, ret_last = self.get_quarter_info(_quarter_this), self.get_quarter_info(_quarter_last)
        logger.info("本期: \n{}\n".format(pprint.pformat(ret_this)))
        logger.info("上期: \n{}\n".format(pprint.pformat(ret_last)))

        if not ret_this or not ret_last:
            return

        # # [临时]拦截数据进行测试
        # ret_last = {}
        # ret_this = {}

        # 计算营业额的阈值 是根据原始数据计算出的值
        operatingrevenue_this, operatingrevenue_last = ret_this.get("OperatingRevenue"), ret_last.get("OperatingRevenue")
        r_threshold = (operatingrevenue_this - operatingrevenue_last) / operatingrevenue_last
        logger.info("营业额同比计算值: {}".format(r_threshold))

        # 计算触发条件 净利润的阈值 是根据原始数据计算出的值
        netprofit_this, netprofit_last = ret_this.get("NPParentCompanyOwners"), ret_last.get("NPParentCompanyOwners")
        threshold = (netprofit_this - netprofit_last) / netprofit_last
        logger.info("归属于母公司净利润同比计算值: {}".format(threshold))

        # 指标触发条件判断
        if netprofit_this > 0 and netprofit_last > 0:
            if threshold >= 0.5:   # 上一期和本期均是盈利的, 盈利增长, 且增长大于 50% >> 触发大幅盈增
                self.inc_50(ret_this, ret_last, threshold, r_threshold)
            elif 0 < threshold < 0.5:   # 上一期和本期均是盈利的, 盈利增长, 但盈利不大于 50% >> 触发增盈
                self.inc(ret_this, ret_last, threshold, r_threshold)
            elif threshold < 0:  # 上期和本期均是盈利的, 盈利减少 >> 触发减盈
                self.reduce(ret_this, ret_last, threshold, r_threshold)

        elif netprofit_this < 0 and netprofit_last > 0:  # 上期盈利, 本期亏损 >> 触发由盈转亏
            self.gain_to_loss(ret_this, ret_last, threshold, r_threshold)
        elif netprofit_this > 0 and netprofit_last < 0:  # 上期亏损, 本期盈利 >> 触发由亏转盈
            self.loss_to_gain(ret_this, ret_last, threshold, r_threshold)
        elif netprofit_this < 0 and netprofit_last < 0 and abs(netprofit_this) < abs(netprofit_last):
            self.ease_loss(ret_this, ret_last, threshold, r_threshold)  # 均亏损 亏损值减少 >> 触发减亏
        elif netprofit_this < 0 and netprofit_last < 0 and abs(netprofit_this) > abs(netprofit_last):
            if threshold > 0.5:  # 均亏损, 亏损值增大,增大幅度大于 50%
                self.intensify_loss_50(ret_this, ret_last, threshold, r_threshold)
            else:  # 均亏损, 亏损值增大, 但不大于 50%
                self.intensify_loss(ret_this, ret_last, threshold, r_threshold)

    def re_percent_data(self, data):
        """处理百分率数据"""
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
        # 保留原始值的符号
        if data > 0:
            flag = 1
        else:
            flag = -1

        data = abs(data)
        if 0 <= data < 10 ** 8:   # 小于 1 亿的钱以万为单位
            data = self.re_ten_thousand_data(data) * flag
            return "{}万".format(data)
        else:
            data = self.re_hundredmillion_data(data) * flag
            return "{}亿".format(data)

    def _process_data(self, ret_this, ret_last, threshold, r_threshold, title_format, content_format, change_type):
        this_end_date = ret_this.get("EndDate")
        quarter_info = """{}年第{}季度""".format(this_end_date.year, self.quarter_map.get(this_end_date.month))

        # 转换为 保留两位的百分数 并且取绝对值 因为前期已经加上了判断增长还是下跌的定语
        threshold = abs(self.re_percent_data(threshold))
        r_threshold = abs(self.re_percent_data(r_threshold))

        # 营业收入的单位从 元 转换为 亿元
        this_operating_revenue = self.re_money_data(ret_this.get("OperatingRevenue"))

        # 净利润的单位 从 元 转换 为 万元
        this_net_profit = self.re_money_data(ret_this.get("NPParentCompanyOwners"))
        last_net_profit = self.re_money_data(ret_last.get("NPParentCompanyOwners"))

        this_basic_EPS = self.re_decimal_data(ret_this.get("BasicEPS"))
        last_basic_EPS = self.re_decimal_data(ret_last.get("BasicEPS"))

        item = dict()
        item['EndDate'] = this_end_date  # 最新一季的时间
        item['InfoPublDate'] = ret_this.get("InfoPublDate")  # 最新一季报表的发布时间
        item['CompanyCode'] = self.company_code
        item['SecuCode'] = self.secu_code
        item['SecuAbbr'] = self.secu_addr
        item['ChangeType'] = change_type
        # 指标参数也保留在生成数据库中
        item['NPParentCompanyOwners'] = ret_this.get("NPParentCompanyOwners")
        title = title_format.format(self.secu_addr, quarter_info, this_net_profit, threshold)
        item['Title'] = title
        content = content_format.format(self.secu_addr, quarter_info, self.secu_addr, quarter_info,
                                        this_operating_revenue, r_threshold,
                                        this_net_profit, threshold,
                                        this_basic_EPS,
                                        last_net_profit, last_basic_EPS)
        item['Content'] = content
        logger.info("\n" + pprint.pformat(item))

    def inc_50(self, ret_this, ret_last, threshold, r_threshold):
        """大幅增盈
        触发条件: 比去年的同期盈利增大 50% 以上; 当日发布季度报告
        """
        key_word = '大幅增盈'
        title_format = key_word + """-{}{}净利{},同比增长{}%"""
        operatingrevenue_str = "增长" if r_threshold > 0 else "下跌"
        content_format = key_word + '''-{}{}业绩:{}{}实现营业收入{},同期''' + operatingrevenue_str \
                         + '''{}%,净利润{}元,同期增长{}%。基本每股收益{}元,上年同期业绩净利润{}元,基本每股收益{}元。'''
        change_type = 1
        self._process_data(ret_this, ret_last, threshold, r_threshold, title_format,
                           content_format, change_type)

    def inc(self, ret_this, ret_last, threshold, r_threshold):
        """增盈
        触发条件: 去年同期盈利，同比去年同期增大盈利，当日发布季度报告
        """
        key_word = "增盈"
        title_format = key_word + """-{}{}净利{}，同比增长{}%"""
        operatingrevenue_str = "增长" if r_threshold > 0 else "下跌"
        content_format = key_word + '''-{}{}业绩:{}{}实现营业收入{}, 同期''' + operatingrevenue_str \
                         + '''{}%,净利润{}元,同期增长{}%。基本每股收益{}元,上年同期业绩净利润{}元, 基本每股收益{}元。'''
        change_type = 2
        self._process_data(ret_this, ret_last, threshold, r_threshold, title_format, content_format, change_type)

    def reduce(self, ret_this, ret_last, threshold, r_threshold):
        """减盈
        触发条件: 去年同期盈利，同比去年同期减小盈利，当日发布季度报告
        """
        key_word = "减盈"
        title_format = key_word + '-{}{}净利{}，同比下跌{}%'
        operatingrevenue_str = "增长" if r_threshold > 0 else "下跌"
        content_format = key_word + '''-{}{}业绩:{}{}实现营业收入{}, 同期''' + operatingrevenue_str \
                         + '''{}%, 净利润{}元, 同期下跌{}%。基本每股收益{}元，上年同期业绩净利润{}元，基本每股收益{}元。'''
        change_type = 3
        self._process_data(ret_this, ret_last, threshold, r_threshold, title_format, content_format, change_type)

    def gain_to_loss(self, ret_this, ret_last, threshold, r_threshold):
        """由盈转亏
        触发条件: 去年同期盈利, 今年同期出现亏损, 当日发布季度报告
        """
        key_word = "由盈转亏"
        title_format = key_word + '-{}{}净利{}，同比下跌{}%'
        operatingrevenue_str = "增长" if r_threshold > 0 else "下跌"
        content_format = key_word + '''-{}{}业绩: {}{}实现营业收入{}, 同期''' + operatingrevenue_str \
                         + '''{}%，净利润{}元，同期下跌{}%。基本每股收益{}元，上年同期业绩净利润{}元，基本每股收益{}元。'''
        change_type = 4
        self._process_data(ret_this, ret_last, threshold, r_threshold, title_format, content_format, change_type)

    def loss_to_gain(self, ret_this, ret_last, threshold, r_threshold):
        """由亏转盈
        触发条件: 去年同期亏损, 今年同期实现盈利, 当日发布季度报告
        """
        key_word = "由亏转盈"
        title_format = key_word + '-{}{}净利{}，同比增长{}%'
        operatingrevenue_str = "增长" if r_threshold > 0 else "下跌"
        content_format = key_word + '''-{}{}业绩:{}{}实现营业收入{}，同期''' + operatingrevenue_str \
                         + '''{}%，净利润{}元，同期增长{}%。基本每股收益{}元，上年同期业绩净利润{}元，基本每股收益{}元。'''
        change_type = 5
        self._process_data(ret_this, ret_last, threshold, r_threshold, title_format, content_format, change_type)

    def ease_loss(self, ret_this, ret_last, threshold, r_threshold):
        """减亏
        触发条件: 去年同期亏损，今年同期亏损减少，当日发布季度报告
        """
        key_word = "减亏"
        title_format = key_word + '-{}{}净利{}, 同比增长{}%'
        operatingrevenue_str = "增长" if r_threshold > 0 else "下跌"
        content_format = key_word + '''-{}{}业绩: {}{}实现营业收入{}, 同期''' + operatingrevenue_str \
                         + '''{}%，净利润{}元, 同期增长{}%。基本每股收益{}元，上年同期业绩净利润{}元，基本每股收益{}元。'''
        change_type = 6
        self._process_data(ret_this, ret_last, threshold, r_threshold, title_format, content_format, change_type)

    def intensify_loss(self, ret_this, ret_last, threshold, r_threshold):
        """增亏
        触发条件: 去年同期亏损, 今年同期亏损增大，当日发布季度报告
        """
        key_word = "增亏"
        title_format = key_word + '-{}{}净利{}，同比下跌{}%'
        operatingrevenue_str = "增长" if r_threshold > 0 else "下跌"
        content_format = key_word + '-{}{}业绩：{}{}实现营业收入{}，同期' + operatingrevenue_str \
                         + '{}%，净利润{}元，同期下跌{}%。基本每股收益{}元，上年同期业绩净利润{}元，基本每股收益{}元。'
        change_type = 7
        self._process_data(ret_this, ret_last, threshold, r_threshold, title_format, content_format, change_type)

    def intensify_loss_50(self, ret_this, ret_last, threshold, r_threshold):
        """大幅增亏
        触发条件: 去年同期亏损，今年同期亏损增大50%以上，当日发布季度报告
        """
        key_word = "大幅增亏"
        title_format = key_word + '-{}{}净利{}，同比下跌:{}%'
        operatingrevenue_str = "增长" if r_threshold > 0 else "下跌"
        content_format = key_word + '-{}{}业绩: {}{}实现营业收入{}, 同期' + operatingrevenue_str \
                         + '下跌{}%,净利润{}元, 同期下跌{}%。基本每股收益{}元，上年同期业绩净利润{}元，基本每股收益{}元。'
        change_type = 8
        self._process_data(ret_this, ret_last, threshold, r_threshold, title_format, content_format, change_type)

    def quarters_diff(self, end_date, last_end_date):
        """判断同一个季度的几次发布是否指标数据差距过大 以 20% 为阈值"""

        pass


if __name__ == "__main__":
    g = GenFiance(3, '000001', '平安银行')
