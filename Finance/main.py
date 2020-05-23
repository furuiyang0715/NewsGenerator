import datetime
import sys

sys.path.append("./../")
from Finance.scanner import Scanner


def task():
    s = Scanner()
    _today = datetime.datetime.combine(datetime.datetime.today(), datetime.time.min)
    _now = datetime.datetime.now()
    s.scan(_today, _now)


if __name__ == "__main__":
    task()
