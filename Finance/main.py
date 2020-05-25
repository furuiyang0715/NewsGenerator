import datetime
import sys

sys.path.append("./../")
from Finance.scanner import Scanner


def task():
    s = Scanner()
    _today = datetime.datetime.combine(datetime.datetime.today(), datetime.time.min)
    _now = datetime.datetime.now()
    s.scan(_today, _now)


def history_task():
    _today = datetime.datetime.combine(datetime.datetime.today(), datetime.time.min)
    _yester_day = _today - datetime.timedelta(days=1)
    # _start = _yester_day - datetime.timedelta(days=180)
    _start = datetime.datetime(2020, 2, 18)

    error_list = []
    _dt = _start
    while _dt < _yester_day:
        try:
            s = Scanner()
            s.scan(_dt, _dt+datetime.timedelta(days=1))
        except:
            error_list.append(_dt)
        _dt = _dt+datetime.timedelta(days=1)

    print(error_list)


if __name__ == "__main__":
    task()

    # history_task()
