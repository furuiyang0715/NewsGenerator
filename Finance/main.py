import sys

sys.path.append("./../")
from Finance.scanner import Scanner


def task():
    s = Scanner()
    s.scan()


if __name__ == "__main__":
    task()
