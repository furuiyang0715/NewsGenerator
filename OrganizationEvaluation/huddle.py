import datetime
import os
import sys

cur_path = os.path.split(os.path.realpath(__file__))[0]
file_path = os.path.abspath(os.path.join(cur_path, ".."))
sys.path.insert(0, file_path)
from base import NewsBase, logger


class OrganizationEvaluation(NewsBase):

    def __init__(self, _today=None):
        super(OrganizationEvaluation, self).__init__()
        self.source_table = 'rrp_rpt_secu_rat'
        self.idx_table = 'stk_quot_idx'
        self.target_table = 'news_generate_organization'
        # 需求是在过 0 点的时候生成前一天的整合新闻
        self._today = datetime.datetime.combine(datetime.datetime.today(), datetime.time.min) if not _today else _today
        self.day = self._today - datetime.timedelta(days=1)
        self.bg_client = None
        self.dc_client = None
        self.target_client = None
        self.title_format = '{}月{}日{}只个股获机构首次评级: '
        self.content_format = '{}（{}）获得{}首次评级 - {}，最新收盘价{}，涨幅{}%。'

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

    def get_pub_first(self):
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
          `Title` varchar(64) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL COMMENT '生成文章标题', 
          `Content` text CHARACTER SET utf8 COLLATE utf8_bin COMMENT '生成文章正文',
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
           PRIMARY KEY (`id`),
           UNIQUE KEY `un2` (`PubDate`, `PubType`) 
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='机构评级资讯生成';
        '''.format(self.target_table)
        self._target_init()
        self.target_client.insert(sql)
        logger.info("建表成功 ")

    def process_items(self, datas):
        secu_codes = self.a_secucategory_codes
        items = []
        for data in datas:
            trd_code = data.get("trd_code")
            if not trd_code in secu_codes:
                logger.info("非A股")
                continue
            secu_sht = data.get("secu_sht")
            com_name = data.get("com_name")
            rat_desc = data.get("rat_desc")

            item = dict()
            if len(trd_code) < 6:
                trd_code = (6-len(trd_code))*"0" + trd_code
            item["SecuCode"] = trd_code
            item['SecuAbbr'] = secu_sht
            inner_code = self.get_inner_code_bysecu(trd_code)
            if not inner_code:
                continue

            sql = '''select Close, ChangePercActual from {} where InnerCode = {} and Date <= '{}' order by Date desc limit 1; 
            '''.format(self.idx_table, inner_code, self.day)
            ret = self.dc_client.select_one(sql)
            _close = self.re_decimal_data(ret.get("Close"))
            changepercactual = self.re_decimal_data(ret.get("ChangePercActual"))
            content = self.content_format.format(secu_sht, trd_code, com_name, rat_desc, _close, changepercactual)
            item['Close'] = _close
            item['ChangePercActual'] = changepercactual
            item['Content'] = content
            logger.debug(item)
            items.append(item)

        title = self.title_format.format(self.day.month, self.day.day, len(items))
        content = ''
        for item in items:
            content += (item.get("Content") + "\n")

        ret = dict()
        ret["PubDate"] = self._today
        ret['PubType'] = 1
        ret['Title'] = title
        ret["Content"] = content
        self._save(self.target_client, ret, self.target_table, ["PubDate", 'PubType', 'Title', "Content"])

    def pub_first_news(self):
        self._create_table()
        datas = self.get_pub_first()
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
    OrganizationEvaluation().pub_first_news()

