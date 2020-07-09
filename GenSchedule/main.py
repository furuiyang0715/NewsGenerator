import time

import schedule

from Funds.days3_top10 import Stocks3DaysTop10
from Funds.limit_up_lb import LimitUpLb


def task_1():
    # 1: 3日净流入前十个股
    Stocks3DaysTop10().start()


def task_2():
    # 2: 连板股今日竞价表现
    LimitUpLb().start()


def main():
    schedule.every().day.at("15:05").do(task_1)

    schedule.every().day.at("09:25").do(task_2)

    while True:
        print("当前调度系统中的任务列表 {}".format(schedule.jobs))
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
