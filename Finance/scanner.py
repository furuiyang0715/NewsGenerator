import datetime
import pprint
import sys

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
        ret = self.juyuan.select_one(sql, company_code)
        return ret

    def scan(self, _today, _now):
        """不断扫描数据库 找出发布时间等于扫描时间的记录"""
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
and InfoPublDate >= '{}' and InfoPublDate < '{}'; '''.format(fields_str, self.source_table, _today, _now)
        logger.info("本次扫描涉及到的查询语句是:\n {}".format(sql))
        ret = self.juyuan.select_all(sql)
        logger.info("本次扫描查询出的个数是:{}".format(len(ret)))

        # 当天无发布数据的
        if not ret:
            return

        # 将查询出的结果先按照公司代码进行分组, 再按照季度节点进行分组
        _map = dict()
        for r in ret:
            company_code = str(r.get("CompanyCode"))
            end_date = r.get("EndDate").strftime("%Y-%m-%d")
            _key = "_".join([company_code, end_date])
            # logger.debug(_key)
            if not _map.get(_key, None):
                _map[_key] = [r, ]
            else:
                _map[_key].append(r)
        # 同一个季度节点的数据 取最早发布的一个
        # 在同一天发布的, IfAdjusted 既有 1 又有 2 的情况下 使用 1 的数据
        # 在以一天内的时间为起止的情况下, 均为在第二种情况
        # print(pprint.pformat(_map))

        # 查询出全部的 A 股公司代码
        company_codes = set(self.total_company_codes().keys())

        for _key, results in _map.items():
            r = None
            if len(results) == 1:
                r = results[0]
            elif len(results) == 2:
                for one in results:
                    if one.get("IfAdjusted") == 1:
                        r = one
            else:
                raise

            if not r:
                logger.warning(len(results))
                logger.warning(pprint.pformat(results))
                raise

            company_code = int(_key.split("_")[0])
            logger.info("{} >> {}".format(company_code, r))
            if company_code not in company_codes:
                logger.warning("财务资讯生成仅仅针对 A 股")
                continue

            _info = self.get_more_info_by_companycode(company_code)
            secu_code, secu_abbr, inner_code = _info.get("SecuCode"), _info.get("SecuAbbr"), _info.get("InnerCode")
            logger.info("证券代码: {}, 证件简称: {}, 聚源内部编码: {}".format(secu_code, secu_abbr, inner_code))
            # 获得扫描出记录的季度时间点
            end_date = r.get("EndDate")
            # 获取同期(上一年)的季度时间点
            last_end_date = datetime.datetime(end_date.year - 1, end_date.month, end_date.day)
            logger.info("本条记录的季度时间节点是{}, 去年同期的时间节点是{}".format(end_date, last_end_date))
            # 实例化一个 GenFiance 类 需要 company_code, secu_code, secu_abbr 三个参数
            code_instance = GenFiance(company_code, secu_code, secu_abbr, inner_code)
            # 判断最新一次生成的数据是否符合条件
            code_instance.diff_quarters(end_date, last_end_date)


# if __name__ == "__main__":
#     s = Scanner()
#     _start = datetime.datetime(2020, 5, 22)
#     _end = datetime.datetime(2020, 5, 23)
#
#     s.scan(_start, _end)
