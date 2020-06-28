import datetime
import os
import sys
import time
import schedule

cur_path = os.path.split(os.path.realpath(__file__))[0]
file_path = os.path.abspath(os.path.join(cur_path, ".."))
sys.path.insert(0, file_path)

from Finance.scanner import Scanner


def task():
    s = Scanner()
    _today = datetime.datetime.combine(datetime.datetime.today(), datetime.time.min)
    _now = datetime.datetime.now()
    s.scan(_today, _now)


def history_task():
    """生成历史资讯数据"""
    _today = datetime.datetime.combine(datetime.datetime.today(), datetime.time.min)
    # 从指定的时间一直生成到昨天
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
    schedule.every(10).minutes.do(task)

    while True:
        schedule.run_pending()
        time.sleep(10)


'''部署 进入根目录下执行
docker build -f DockerfileUseApi2p -t registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/newsgenerator:v2 .
docker push registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/newsgenerator:v2
sudo docker pull registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/newsgenerator:v2

sudo docker run --log-opt max-size=10m --log-opt max-file=3 -itd \
--env LOCAL=0 \
--name generate_finance \
registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/newsgenerator:v2 \
python Finance/main.py
'''