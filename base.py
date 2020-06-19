import base64
import hashlib
import hmac
import json
import logging
import sys
import time
import traceback

import requests
import urllib.parse

sys.path.append("./../")
from configs import (SPIDER_MYSQL_HOST, SPIDER_MYSQL_PORT, SPIDER_MYSQL_USER, SPIDER_MYSQL_PASSWORD,
                     SPIDER_MYSQL_DB, PRODUCT_MYSQL_HOST, PRODUCT_MYSQL_PORT, PRODUCT_MYSQL_USER,
                     PRODUCT_MYSQL_PASSWORD, PRODUCT_MYSQL_DB, JUY_HOST, JUY_PORT, JUY_USER, JUY_PASSWD,
                     JUY_DB, DC_HOST, DC_PORT, DC_USER, DC_PASSWD, DC_DB, SECRET, TOKEN, LOCAL, BG_HOST, BG_PORT,
                     BG_USER, BG_PASSWD, BG_DB)
from sql_pool import PyMysqlPoolBase

if LOCAL:
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
else:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class NewsBase(object):

    spider_cfg = {  # 爬虫库
        "host": SPIDER_MYSQL_HOST,
        "port": SPIDER_MYSQL_PORT,
        "user": SPIDER_MYSQL_USER,
        "password": SPIDER_MYSQL_PASSWORD,
        "db": SPIDER_MYSQL_DB,
    }

    product_cfg = {  # 正式库
        "host": PRODUCT_MYSQL_HOST,
        "port": PRODUCT_MYSQL_PORT,
        "user": PRODUCT_MYSQL_USER,
        "password": PRODUCT_MYSQL_PASSWORD,
        "db": PRODUCT_MYSQL_DB,
    }

    # 聚源数据库
    juyuan_cfg = {
        "host": JUY_HOST,
        "port": JUY_PORT,
        "user": JUY_USER,
        "password": JUY_PASSWD,
        "db": JUY_DB,
    }

    # 贝格数据库
    bigdata_cfg = {
        "host": BG_HOST,
        "port": BG_PORT,
        "user": BG_USER,
        "password": BG_PASSWD,
        "db": BG_DB,
    }

    # 数据中心库
    dc_cfg = {
        "host": DC_HOST,
        "port": DC_PORT,
        "user": DC_USER,
        "password": DC_PASSWD,
        "db": DC_DB,
    }

    def __init__(self):
        self.dc_client = None
        self.target_client = None
        self.juyuan_client = None

    def _dc_init(self):
        if not self.dc_client:
            self.dc_client = self._init_pool(self.dc_cfg)

    def _target_init(self):
        if not self.target_client:
            self.target_client = self._init_pool(self.product_cfg)

    def _juyuan_init(self):
        if not self.juyuan_client:
            self.juyuan_client = self._init_pool(self.juyuan_cfg)

    def __del__(self):
        if self.dc_client:
            self.dc_client.dispose()
        if self.target_client:
            self.target_client.dispose()
        if self.juyuan_client:
            self.juyuan_client.dispose()

    def _init_pool(self, cfg: dict):
        """
        eg.
        conf = {
                "host": LOCAL_MYSQL_HOST,
                "port": LOCAL_MYSQL_PORT,
                "user": LOCAL_MYSQL_USER,
                "password": LOCAL_MYSQL_PASSWORD,
                "db": LOCAL_MYSQL_DB,
        }
        :param cfg:
        :return:
        """
        pool = PyMysqlPoolBase(**cfg)
        return pool

    def get_inner_code_bysecu(self, secu_code):
        """通过证券代码获取聚源内部编码"""
        ret = self.inner_code_map.get(secu_code)
        if not ret:
            logger.warning("此证券代码 {} 不存在内部编码".format(secu_code))
            # raise
            return None
        return ret

    def get_inner_code_bycompany(self, company_code):
        """通过公司代码获取聚源内部编码"""
        ret = self.inner_company_code_map.get(company_code)
        if not ret:
            logger.warning("此公司编码 {} 不存在内部编码 ".format(company_code))
            raise
        return ret

    @property
    def inner_code_map(self):
        """
        获取 证券代码: 聚源内部编码 映射表
        https://dd.gildata.com/#/tableShow/27/column///
        https://dd.gildata.com/#/tableShow/718/column///
        """
        juyuan = self._init_pool(self.juyuan_cfg)
        # 8 是开放式基金
        sql = 'SELECT SecuCode,InnerCode from SecuMain WHERE SecuCategory in (1, 2, 8) and SecuMarket in (83, 90) and ListedSector in (1, 2, 6, 7);'
        ret = juyuan.select_all(sql)
        juyuan.dispose()
        info = {}
        for r in ret:
            key = r.get("SecuCode")
            value = r.get('InnerCode')
            info[key] = value
        return info

    @property
    def inner_company_code_map(self):
        """
        获取 公司编码: 聚源内部编码 映射表
        """
        juyuan = self._init_pool(self.juyuan_cfg)
        sql = '''select CompanyCode, InnerCode from SecuMain WHERE SecuCategory in (1, 2, 8) and SecuMarket in (83, 90) and ListedSector in (1, 2, 6, 7);'''
        ret = juyuan.select_all(sql)
        juyuan.dispose()
        info = {}
        for r in ret:
            key = r.get("CompanyCode")
            value = r.get('InnerCode')
            info[key] = value
        return info

    def ding(self, msg):
        def get_url():
            timestamp = str(round(time.time() * 1000))
            secret_enc = SECRET.encode('utf-8')
            string_to_sign = '{}\n{}'.format(timestamp, SECRET)
            string_to_sign_enc = string_to_sign.encode('utf-8')
            hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
            sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
            url = 'https://oapi.dingtalk.com/robot/send?access_token={}&timestamp={}&sign={}'.format(
                TOKEN, timestamp, sign)
            return url

        url = get_url()
        header = {
            "Content-Type": "application/json",
            "Charset": "UTF-8"
        }
        message = {
            "msgtype": "text",
            "text": {
                "content": "{}@15626046299".format(msg)
            },
            "at": {
                "atMobiles": [
                    "15626046299",
                ],
                "isAtAll": False
            }
        }
        message_json = json.dumps(message)
        resp = requests.post(url=url, data=message_json, headers=header)
        if resp.status_code == 200:
            logger.info("钉钉发送消息成功: {}".format(msg))
        else:
            logger.warning("钉钉消息发送失败")

    def total_company_codes(self):
        """获取涵盖在统计范围内的全部公司代码 按照文档的写法 是只需要 A 股的"""
        juyuan = self._init_pool(self.juyuan_cfg)
        # sql = '''select CompanyCode, SecuCode, InnerCode from secumain where SecuCategory = 1; '''
        sql = '''SELECT InnerCode,CompanyCode,SecuCode,SecuAbbr FROM secumain WHERE SecuCategory=1 \
AND SecuMarket IN(83,90) AND ListedState=1 ORDER BY SecuCode DESC;
        '''
        ret = juyuan.select_all(sql)
        _map = {}
        for r in ret:
            company_code = r.get("CompanyCode")
            _map[company_code] = (r.get("SecuCode"), r.get("InnerCode"))

        return _map

    @property
    def a_secucategory_codes(self):
        """获取 A 股证券代码列表"""
        juyuan = self._init_pool(self.juyuan_cfg)
        sql = '''select SecuCode from secumain where SecuCategory = 1;'''
        ret = juyuan.select_all(sql)
        secu_codes = []
        for r in ret:
            secu_codes.append(r.get("SecuCode"))
        return secu_codes

    def get_juyuan_codeinfo(self, secu_code):
        self._juyuan_init()
        sql = 'SELECT SecuCode,InnerCode, SecuAbbr from SecuMain WHERE SecuCategory in (1, 2, 8) \
and SecuMarket in (83, 90) \
and ListedSector in (1, 2, 6, 7) and SecuCode = "{}";'.format(secu_code)
        ret = self.juyuan_client.select_one(sql)
        return ret.get('InnerCode'), ret.get("SecuAbbr")

    @staticmethod
    def re_decimal_data(data):
        """一般小数保留前两位"""
        ret = float("%.2f" % data)
        return ret

    def contract_sql(self, datas, table: str, update_fields: list):
        if not isinstance(datas, list):
            datas = [datas, ]

        to_insert = datas[0]
        ks = []
        vs = []
        for k in to_insert:
            ks.append(k)
            vs.append(to_insert.get(k))
        fields_str = "(" + ",".join(ks) + ")"
        values_str = "(" + "%s," * (len(vs) - 1) + "%s" + ")"
        base_sql = '''INSERT INTO `{}` '''.format(table) + fields_str + ''' values ''' + values_str

        params = []
        for data in datas:
            vs = []
            for k in ks:
                vs.append(data.get(k))
            params.append(vs)

        if update_fields:
            # https://stackoverflow.com/questions/12825232/python-execute-many-with-on-duplicate-key-update/12825529#12825529
            # sql = 'insert into A (id, last_date, count) values(%s, %s, %s) on duplicate key update last_date=values(last_date),count=count+values(count)'
            on_update_sql = ''' ON DUPLICATE KEY UPDATE '''
            for update_field in update_fields:
                on_update_sql += '{}=values({}),'.format(update_field, update_field)
            on_update_sql = on_update_sql.rstrip(",")
            sql = base_sql + on_update_sql + """;"""
        else:
            sql = base_sql + ";"
        return sql, params

    def _batch_save(self, sql_pool, to_inserts, table, update_fields):
        try:
            sql, values = self.contract_sql(to_inserts, table, update_fields)
            count = sql_pool.insert_many(sql, values)
        except:
            traceback.print_exc()
            logger.warning("失败")
        else:
            logger.info("批量插入的数量是{}".format(count))
            sql_pool.end()
            return count

    def _save(self, sql_pool, to_insert, table, update_fields):
        try:
            insert_sql, values = self.contract_sql(to_insert, table, update_fields)
            value = values[0]
            count = sql_pool.insert(insert_sql, value)
        except:
            traceback.print_exc()
            logger.warning("失败")
        else:
            if count == 1:
                logger.info("插入新数据 {}".format(to_insert))
            elif count == 2:
                logger.info("刷新数据 {}".format(to_insert))
            else:
                logger.info("已有数据 {} ".format(to_insert))
            sql_pool.end()
            return count

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

    def re_hundredmillion_data(self, data):
        """将元转换为亿元 并且保留两位小数"""
        ret = float("%.2f" % (data / 10**8))
        return ret

    def re_ten_thousand_data(self, data):
        """将元转换为万元 并且保留两位小数"""
        ret = float("%.2f" % (data / 10**4))
        return ret

    def create_sql_table(self):
        # TODO 进行类似于 sqlalchemy 的简单封装
        pass
