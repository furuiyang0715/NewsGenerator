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
                     JUY_DB, DC_HOST, DC_PORT, DC_USER, DC_PASSWD, DC_DB, SECRET, TOKEN, LOCAL)
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

    # 数据中心库
    dc_cfg = {
        "host": DC_HOST,
        "port": DC_PORT,
        "user": DC_USER,
        "password": DC_PASSWD,
        "db": DC_DB,
    }

    def __init__(self):
        pass

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

    def contract_sql(self, to_insert: dict, table: str, update_fields: list):
        ks = []
        vs = []
        for k in to_insert:
            ks.append(k)
            vs.append(to_insert.get(k))
        fields_str = "(" + ",".join(ks) + ")"
        values_str = "(" + "%s," * (len(vs) - 1) + "%s" + ")"
        base_sql = '''INSERT INTO `{}` '''.format(table) + fields_str + ''' values ''' + values_str

        # 是否在主键冲突时进行更新插入
        if update_fields:
            on_update_sql = ''' ON DUPLICATE KEY UPDATE '''
            update_vs = []
            for update_field in update_fields:
                on_update_sql += '{}=%s,'.format(update_field)
                update_vs.append(to_insert.get(update_field))
            on_update_sql = on_update_sql.rstrip(",")
            sql = base_sql + on_update_sql + """;"""
            vs.extend(update_vs)
        else:
            sql = base_sql + """;"""

        return sql, tuple(vs)

    def _save(self, sql_pool, to_insert, table, update_fields):
        try:
            insert_sql, values = self.contract_sql(to_insert, table, update_fields)
            count = sql_pool.insert(insert_sql, values)
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
                pass

            sql_pool.end()
            return count

    def get_inner_code_bysecu(self, secu_code):
        """通过证券代码获取聚源内部编码"""
        ret = self.inner_code_map.get(secu_code)
        if not ret:
            logger.warning("此证券代码 {} 不存在内部编码".format(secu_code))
            raise
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
        sql = '''select CompanyCode, SecuCode, InnerCode from secumain where SecuCategory = 1; '''
        ret = juyuan.select_all(sql)
        _map = {}
        for r in ret:
            company_code = r.get("CompanyCode")
            _map[company_code] = (r.get("SecuCode"), r.get("InnerCode"))

        return _map
