import datetime
import pprint

from Finance.base import NewsBase, logger
from Finance.gen_finance_news import GenFiance


class Scanner(NewsBase):
    def __init__(self):
        super(Scanner, self).__init__()
        self.source_table = 'LC_IncomeStatementAll'
        self.juyuan = None

    def __del__(self):
        if self.juyuan:
            self.juyuan.dispose()

    def get_more_info_by_companycode(self, company_code):

        sql = '''select SecuCode, SecuAbbr, InnerCode from secumain where CompanyCode = %s; '''
        ret = self.juyuan.select_one(sql,company_code)
        return ret

    def scan(self):
        """不断扫描数据库 找出发布时间等于扫描时间的记录"""
        _today = datetime.datetime.combine(datetime.datetime.today(), datetime.time.min)
        _now = datetime.datetime.now()
        # 初始化连接池
        self.juyuan = self._init_pool(self.juyuan_cfg)

        # 与产品沟通之后 这里做了一个调整 就是将"净利润"改为"归属于母公司所有者的净利润"
        # NetProfit --> NPParentCompanyOwners
        fields_str = "CompanyCode, EndDate, InfoPublDate, IfAdjusted, IfMerged, NPParentCompanyOwners, OperatingRevenue, BasicEPS"
        sql = '''select {} from {} where IfMerged=1 \
and NetProfit is not NULL \
and OperatingRevenue is not null \
and BasicEPS is not null \
and IfAdjusted in (1,2) \
and InfoPublDate >= '{}' and InfoPublDate <= '{}'; '''.format(fields_str, self.source_table, _today, _now)
        logger.info("本次扫描涉及到的查询语句是:\n {}".format(sql))
        ret = self.juyuan.select_all(sql)
        logger.info("本次扫描查询出的个数是:{}".format(len(ret)))
        for r in ret:
            logger.info("\n{}".format(pprint.pformat(r)))
            # 根据公司代码获取证券代码、证券简称以及聚源内部编码
            company_code = r.get("CompanyCode")
            _info = self.get_more_info_by_companycode(company_code)
            secu_code, secu_abbr, inner_code = _info.get("SecuCode"), _info.get("SecuAbbr"), _info.get("InnerCode")
            logger.info("证券代码: {}, 证件简称: {}, 聚源内部编码: {}".format(secu_code, secu_abbr, inner_code))
            # 获得扫描出记录的季度时间点
            end_date = r.get("EndDate")
            # 获取同期(上一年)的季度时间点
            last_end_date = datetime.datetime(end_date.year - 1, end_date.month, end_date.day)
            logger.info("本条记录的季度时间节点是{}, 去年同期的时间节点是{}".format(end_date, last_end_date))
            # 实例化一个 GenFiance 类 需要 company_code, secu_code, secu_abbr 三个参数
            code_instance = GenFiance(company_code, secu_code, secu_abbr)
            # step1 判断最新一次生成的数据是否符合条件
            code_instance.diff_quarters(end_date, last_end_date)
            # TODO step2 判断同一个季度的几次发布是否指标数据差距过大 以 20% 为阈值
            code_instance.quarters_diff(end_date, last_end_date)


if __name__ == "__main__":
    s = Scanner()
    s.scan()
