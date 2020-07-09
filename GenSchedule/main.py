import datetime
import functools
import os
import pprint
import sys
import time
import traceback

import schedule

cur_path = os.path.split(os.path.realpath(__file__))[0]
file_path = os.path.abspath(os.path.join(cur_path, ".."))
sys.path.insert(0, file_path)

from Finance.scanner import Scanner
from Funds.days3_top10 import Stocks3DaysTop10
from Funds.limit_up_lb import LimitUpLb
from Funds.morning_top10 import MorningTop10
from Funds.north_fund import NorthFund
from Funds.open_unusual import OpenUnusual
from OrganizationEvaluation.huddle import OrganizationEvaluation
from WinnersList.winlist_api import OraApi

from base import logger, NewsBase


def catch_exceptions(cancel_on_failure=False):
    def catch_exceptions_decorator(job_func):
        @functools.wraps(job_func)
        def wrapper(*args, **kwargs):
            try:
                return job_func(*args, **kwargs)
            except:
                logger.warning(traceback.format_exc())
                # 在此处发送钉钉消息
                if cancel_on_failure:
                    logger.warning("异常 任务结束: {}".format(schedule.CancelJob))
                    schedule.cancel_job(job_func)
                    return schedule.CancelJob
        return wrapper
    return catch_exceptions_decorator


@catch_exceptions(cancel_on_failure=False)
def task_finance():
    s = Scanner()
    _today = datetime.datetime.combine(datetime.datetime.today(), datetime.time.min)
    _now = datetime.datetime.now()
    s.scan(_today, _now)


@catch_exceptions(cancel_on_failure=False)
def task_flownorth():
    north = NorthFund()
    while True:
        _now = datetime.datetime.now()
        day_start = datetime.datetime(_now.year, _now.month, _now.day, 9, 25, 0)
        day_end = datetime.datetime(_now.year, _now.month, _now.day, 15, 5, 0)
        if _now <= day_start or _now >= day_end:
            print("不在生成时间内 {}".format(_now))
            time.sleep(30)
        else:
            north.start(_now)
            time.sleep(10)


@catch_exceptions(cancel_on_failure=False)
def task_1():
    # 1: 3日净流入前十个股
    Stocks3DaysTop10().start()


@catch_exceptions(cancel_on_failure=False)
def task_2():
    # 2: 连板股今日竞价表现
    LimitUpLb().start()


@catch_exceptions(cancel_on_failure=False)
def task_3():
    # 3: 早盘主力十大净买个股
    MorningTop10().start()


@catch_exceptions(cancel_on_failure=False)
def task_4():
    # 4:  开盘异动盘口
    OpenUnusual().start()


@catch_exceptions(cancel_on_failure=False)
def task_5_6():
    # 5: 机构首次评级
    # 6: 获多机构买入增持评级
    runner = OrganizationEvaluation()
    runner.pub_first_news()
    runner.evaluate_more()


@catch_exceptions(cancel_on_failure=False)
def task_7_8():
    # 7:  龙虎榜-机构净买额最大
    # 8:  龙虎榜-机构席位最多
    OraApi().start()


def task_info():
    bs = NewsBase()
    bs._dc_init()
    _client = bs.dc_client

    # bs._test_init()
    # _client = bs.test_client

    today_str = (datetime.datetime.combine(datetime.datetime.today(), datetime.time.min)).strftime("%Y-%m-%d")
    base_sql = '''select * from news_generate where NewsType = {} and Date >= '{}'; '''

    imap = {}
    for news_type in range(1, 9):
        sql = base_sql.format(news_type, today_str)
        # print(sql)
        ret = _client.select_one(sql)
        title = ret.get("Title") if ret else ''
        # print(title)
        imap[news_type] = title

    print(pprint.pformat(imap))
    bs.ding(pprint.pformat(imap))


def main():
    schedule.every().day.at("15:05").do(task_1)

    schedule.every().day.at("09:25").do(task_2)

    schedule.every().day.at("10:30").do(task_3)

    schedule.every().day.at("09:36").do(task_4)

    schedule.every().day.at("00:05").do(task_5_6)
    schedule.every().day.at("09:00").do(task_5_6)

    # TODO 测试大概的更新时间
    schedule.every().day.at("15:30").do(task_7_8)
    schedule.every().day.at("18:06").do(task_7_8)

    schedule.every(10).minutes.do(task_finance)

    north = NorthFund()
    while True:
        print("当前调度系统中的任务列表 {}".format(schedule.jobs))
        schedule.run_pending()

        _now = datetime.datetime.now()
        day_start = datetime.datetime(_now.year, _now.month, _now.day, 9, 25, 0)
        day_end = datetime.datetime(_now.year, _now.month, _now.day, 15, 5, 0)
        if _now <= day_start or _now >= day_end:
            print("不在生成时间内 {}".format(_now))
            time.sleep(30)
        else:
            north.start(_now)
            time.sleep(10)


if __name__ == "__main__":
    # main()

    task_info()


'''
docker build -f DockerfileUseApi2p -t registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/newsgenerator:v2 . 
docker push registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/newsgenerator:v2 
sudo docker pull registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/newsgenerator:v2

sudo docker run --log-opt max-size=10m --log-opt max-file=3 -itd \
--env LOCAL=0 \
--name gen_all \
registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/newsgenerator:v2 \
python GenSchedule/main.py

'''
