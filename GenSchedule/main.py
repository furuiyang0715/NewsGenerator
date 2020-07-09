import time

import schedule

from Funds.days3_top10 import Stocks3DaysTop10
from Funds.limit_up_lb import LimitUpLb
from Funds.morning_top10 import MorningTop10
from Funds.open_unusual import OpenUnusual
from OrganizationEvaluation.huddle import OrganizationEvaluation


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


def main():
    schedule.every().day.at("15:05").do(task_1)

    schedule.every().day.at("09:25").do(task_2)

    schedule.every().day.at("10:30").do(task_3)

    schedule.every().day.at("09:36").do(task_4)

    schedule.every().day.at("00:05").do(task_5_6)
    schedule.every().day.at("09:00").do(task_5_6)

    while True:
        print("当前调度系统中的任务列表 {}".format(schedule.jobs))
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
