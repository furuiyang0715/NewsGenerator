'''

select CompanyCode, EndDate, InfoPublDate, IfAdjusted, IfMerged, NetProfit, OperatingRevenue, BasicEPS \
from LC_IncomeStatementAll where IfMerged=1 and NetProfit is not NULL and OperatingRevenue is not null \
and BasicEPS is not null and IfAdjusted in (1,2) and InfoPublDate >= '2020-05-21 00:00:00' and InfoPublDate <= '2020-05-21 15:17:36.362691';

select CompanyCode, EndDate, InfoPublDate, IfAdjusted, IfMerged, NetProfit, OperatingRevenue, BasicEPS \
from LC_IncomeStatementAll where CompanyCode = 92 and EndDate = '2019-12-31' \
and NetProfit is not NULL and OperatingRevenue is not null \
and BasicEPS is not null;

select CompanyCode, EndDate, InfoPublDate, IfAdjusted, IfMerged, NetProfit, OperatingRevenue, BasicEPS \
from LC_IncomeStatementAll where CompanyCode = 92 and EndDate = '2019-12-31';

'''
