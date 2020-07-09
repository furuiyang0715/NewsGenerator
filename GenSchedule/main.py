import time

import schedule

from Funds.days3_top10 import Stocks3DaysTop10
from Funds.limit_up_lb import LimitUpLb
from Funds.morning_top10 import MorningTop10
from Funds.open_unusual import OpenUnusual


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


def main():
    schedule.every().day.at("15:05").do(task_1)

    schedule.every().day.at("09:25").do(task_2)

    schedule.every().day.at("10:30").do(task_3)

    schedule.every().day.at("09:36").do(task_4)

    while True:
        print("当前调度系统中的任务列表 {}".format(schedule.jobs))
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
