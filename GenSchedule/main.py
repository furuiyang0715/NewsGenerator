import time

import schedule

from Funds.days3_top10 import Stocks3DaysTop10


def task_1():
    # 1: 3日净流入前十个股
    Stocks3DaysTop10().start()

    #


def main():
    schedule.every().day.at("15:05").do(task_1)

    while True:
        print("当前调度系统中的任务列表 {}".format(schedule.jobs))
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
