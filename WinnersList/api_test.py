import json
import pprint

import requests

url = 'http://bg.jingzhuan.cn?api=stock_rank_INS_Up&timeperiod=1'

resp = requests.get(url)
if resp.status_code == 200:
    body = resp.text
    datas = json.loads(body)
    print(pprint.pformat(datas))
else:
    print(resp)


'''
 'response': [{
'END_DT': '2020-06-24 00:00:00',
'INSBUY': '4',
'INSSELL': '2',
'INSUP': '6',
'PREID': '5.48471658',
'SUMBUY': '32152878.0000',        # 总买入 
'SUMNETBUY': '20672612.0000',     # 净买入 
'SUMSELL': '11480266.0000',       # 总卖出
'TRD_CODE': '002977',
'userdata': '002977', 
},
'''

'''
message xwmm_vary_data
{
optional string code = 1;        // 股票代码
optional uint64 time = 2;        // 异动时间
optional double rise_rate = 3;  // 当日涨幅
optional double close = 4;      // 当日价格
repeated int32 abn_type = 5;   // 异动类型 [展示的时候进行列展开]
optional double tnv_rate = 6;   // 换收率
optional double net_buy = 7;    // 净买入
optional double sum_buy = 8;    // 总买入
optional double buy_rate = 9;   // 买入占比
optional double sum_sell = 10;  // 总卖出
optional double sell_rate = 11; // 卖出占比
optional double tnv_val =  12;  //  成交额
optional int32 org_count = 13; // 机构数量
optional double org_net_buy = 14; // 机构净买
repeated int32 faction_operator = 15; // 帮派操作; 1 联营 2 协同
repeated string faction_join = 16; // 参与帮派
optional double faction_net_buy = 17; // 帮派净买
optional double lgt_net_buy = 18;  // 陆股通净买
optional int32 abn_day = 19; // 异动天数 1,3
optional string industry_block_code = 20; // 所属行业板块代码
#    optional string industry_block_name = 21; // 所属行业板块名称
}
'''
