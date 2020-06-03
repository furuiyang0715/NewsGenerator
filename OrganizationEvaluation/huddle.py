# 机构首次生成合并生成
import datetime
import os
import sys

cur_path = os.path.split(os.path.realpath(__file__))[0]
file_path = os.path.abspath(os.path.join(cur_path, ".."))
sys.path.insert(0, file_path)
from base import NewsBase, logger


class OrganizationEvaluation(NewsBase):

    def __init__(self):
        super(OrganizationEvaluation, self).__init__()
        self.source_table = 'rrp_rpt_secu_rat'
        self.idx_table = 'stk_quot_idx'
        self.target_table = 'news_generate_organization'
        # 需求是在过 0 点的时候生成前一天的整合新闻
        self.day = datetime.datetime.combine(datetime.datetime.today(), datetime.time.min) - datetime.timedelta(days=1)
        self.bg_client = None
        self.dc_client = None
        self.target_client = None
        self.title_format = "{}今日获机构首次评级-{}"
        self.content_format = '{}月{}日{}（{}）获得{}首次评级 - {}，最新收盘价{}，涨幅{}%。'
        self.fields = ['PubDate',     # 资讯发布时间, 首次评级立即发布; 获多机构买入增持评级, 第二天 9 点发出
                       'PubType',     # 资讯类型1:首次评级2:获多机构买入增持评级
                       'SecuCode',    # 证券代码
                       'SecuAbbr',   # 证券简称
                       'InnerCode',  # 聚源内部编码
                       'ComId',     # 撰写机构编码 仅在某机构首次评级时填入
                       'ComName',   # 撰写机构名称 仅在某机构首次评级时填入
                       'RatCode',   # 投资评级代码 仅在某机构首次评级时填入
                       'RatDesc',   # 投资评级描述 仅在某机构首次评级时填入
                       'Title',    # 生成资讯标题
                       'Content',  # 生层资讯正文
                       'Close',  # 收盘价
                       'ChangePercActual',  # 实际涨跌幅
                    ]

    def __del__(self):
        if self.bg_client:
            self.bg_client.dispose()
        if self.dc_client:
            self.dc_client.dispose()
        if self.target_client:
            self.target_client.dispose()

    def _bg_init(self):
        self.bg_client = self._init_pool(self.bigdata_cfg)

    def _dc_init(self):
        self.dc_client = self._init_pool(self.dc_cfg)

    def _target_init(self):
        self.target_client = self._init_pool(self.product_cfg)

    def get_pub_today(self):
        """发布时间在指定日期的全部数据"""
        self._bg_init()
        select_fields = 'pub_dt,trd_code,secu_sht, com_id,com_name,rat_code,rat_desc'
        sql = '''select {} from {} where pub_dt = '{}';'''.format(select_fields, self.source_table, self.day)
        ret = self.bg_client.select_all(sql)
        return ret

    def check_pub_first(self, data):
        """判断是否是机构首次发布"""
        sql = '''select com_id,trd_code,pub_dt from {} where com_id = '{}' and trd_code = '{}' and pub_dt < '{}'; 
        '''.format(self.source_table, data.get("com_id"), data.get("trd_code"), data.get("pub_dt"))
        ret = self.bg_client.select_all(sql)
        if not ret:
            return True
        else:
            return False

    def _create_table(self):
        sql = '''
        CREATE TABLE IF NOT EXISTS `{}` (
          `id` int(11) NOT NULL AUTO_INCREMENT,
          `PubDate` datetime NOT NULL COMMENT '资讯发布时间', 
          `PubType` int NOT NULL COMMENT '资讯类型1:首次评级2:获多机构买入增持评级',
          `SecuCode` varchar(10) DEFAULT NULL COMMENT '证券代码',  
          `SecuAbbr` varchar(100) DEFAULT NULL COMMENT '证券简称', 
          `InnerCode` int(11) NOT NULL COMMENT '证券内部编码',
          `ComId` decimal(10,0) NOT NULL COMMENT '发行机构 ID',
          `ComName` varchar(200) DEFAULT NULL COMMENT '撰写机构名称', 
          `RatCode` decimal(10,0) DEFAULT NULL COMMENT '投资评级代码',
          `RatDesc` varchar(50) DEFAULT NULL COMMENT '投资评级描述',
          `Title` varchar(64) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL COMMENT '生成文章标题', 
          `Content` text CHARACTER SET utf8 COLLATE utf8_bin COMMENT '生成文章正文',
          `Close` decimal(20,2) DEFAULT NULL COMMENT '收盘价',
          `ChangePercActual` decimal(20,6) DEFAULT NULL COMMENT '实际涨跌幅(%)', 
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
           PRIMARY KEY (`id`),
           UNIQUE KEY `un2` (`SecuCode`, `PubDate`, `PubType`, `ComId`) 
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='机构评级资讯生成';
        '''.format(self.target_table)
        self._target_init()
        self.target_client.insert(sql)
        logger.info("建表成功 ")

    def process_items(self, datas):
        items = []
        for data in datas:
            trd_code = data.get("trd_code")
            secu_sht = data.get("secu_sht")
            com_id = data.get("com_id")
            com_name = data.get("com_name")
            rat_code = data.get("rat_code")
            rat_desc = data.get("rat_desc")

            item = dict()
            item['PubDate'] = self.day
            item['PubType'] = 1
            if len(trd_code) < 6:
                trd_code = (6-len(trd_code))*"0" + trd_code
            item["SecuCode"] = trd_code
            item['SecuAbbr'] = secu_sht
            inner_code = self.get_inner_code_bysecu(trd_code)
            if not inner_code:
                continue
            item['InnerCode'] = inner_code
            item['ComId'] = com_id
            item['ComName'] = com_name
            item['RatCode'] = rat_code
            item['RatDesc'] = rat_desc

            sql = '''select Close, ChangePercActual from {} where InnerCode = {} and Date <= '{}' order by Date desc limit 1; 
            '''.format(self.idx_table, inner_code, self.day)    # 因为假如今天被机构首次评级 最新拿到的是昨天的行情数据
            ret = self.dc_client.select_one(sql)
            _close = self.re_decimal_data(ret.get("Close"))
            changepercactual = self.re_decimal_data(ret.get("ChangePercActual"))
            # eg.4月12日杰瑞股份（000021）获得申港证券首次评级 - 买入，最新收盘价24.86，涨幅+1.12%。
            # '{}月{}日{}（{}）获得{}首次评级 - {}，最新收盘价{}，涨幅{}%。'
            content = self.content_format.format(self.day.month, self.day.day, secu_sht, trd_code, com_name, rat_desc, _close, changepercactual)
            item['Close'] = _close
            item['ChangePercActual'] = changepercactual
            item['Content'] = content
            logger.info(item)
            items.append(item)

        # print(items)

    def start(self):
        datas = self.get_pub_today()
        logger.info("{} 的发布个数为 {}".format(self.day, len(datas)))

        self._dc_init()
        first_datas = []
        for data in datas:
            is_first = self.check_pub_first(data)
            if is_first:
                first_datas.append(data)
                # logger.info(data)
        if datas:
            logger.info("{} 首次发布的个数 {}".format(self.day, len(first_datas)))

        self.process_items(first_datas)


if __name__ == "__main__":
    OrganizationEvaluation().start()

