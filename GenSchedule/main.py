import datetime
import time

import schedule

from Finance.scanner import Scanner
from Funds.days3_top10 import Stocks3DaysTop10
from Funds.limit_up_lb import LimitUpLb
from Funds.morning_top10 import MorningTop10
from Funds.north_fund import NorthFund
from Funds.open_unusual import OpenUnusual
from OrganizationEvaluation.huddle import OrganizationEvaluation
from WinnersList.winlist_api import OraApi


def task_finance():
    s = Scanner()
    _today = datetime.datetime.combine(datetime.datetime.today(), datetime.time.min)
    _now = datetime.datetime.now()
    s.scan(_today, _now)


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


def task_1():
    # 1: 3日净流入前十个股
    Stocks3DaysTop10().start()


def task_2():
    # 2: 连板股今日竞价表现
    LimitUpLb().start()


def task_3():
    # 3: 早盘主力十大净买个股
    MorningTop10().start()


def task_4():
    # 4:  开盘异动盘口
    OpenUnusual().start()


def task_5_6():
    # 5: 机构首次评级
    # 6: 获多机构买入增持评级
    runner = OrganizationEvaluation()
    runner.pub_first_news()
    runner.evaluate_more()


def task_7_8():
    # 7:  龙虎榜-机构净买额最大
    # 8:  龙虎榜-机构席位最多
    OraApi().start()


def main():
    schedule.every().day.at("15:05").do(task_1)

    schedule.every().day.at("09:25").do(task_2)

    schedule.every().day.at("10:30").do(task_3)

    schedule.every().day.at("09:36").do(task_4)

    schedule.every().day.at("00:05").do(task_5_6)
    schedule.every().day.at("09:00").do(task_5_6)

    # TODO 测试大概的更新时间
    schedule.every().day.at("16:06").do(task_7_8)
    schedule.every().day.at("17:06").do(task_7_8)
    schedule.every().day.at("18:06").do(task_7_8)

    schedule.every(10).minutes.do(task_finance)

    # while True:
    #     print("当前调度系统中的任务列表 {}".format(schedule.jobs))
    #     schedule.run_pending()
    #     time.sleep(10)

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
    main()
