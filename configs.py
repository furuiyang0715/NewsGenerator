import configparser
import os

env = os.environ

cf = configparser.ConfigParser()
thisdir = os.path.dirname(__file__)
cf.read(os.path.join(thisdir, '.conf'))

# datacenter
DC_HOST = env.get("DC_HOST", cf.get('dc', 'DC_HOST'))
DC_PORT = int(env.get("DC_PORT", cf.get('dc', 'DC_PORT')))
DC_USER = env.get("DC_USER", cf.get('dc', 'DC_USER'))
DC_PASSWD = env.get("DC_PASSWD", cf.get('dc', 'DC_PASSWD'))
DC_DB = env.get("DC_DB", cf.get('dc', 'DC_DB'))

# deploy
LOCAL = bool(int(env.get("LOCAL", cf.get('deploy', 'LOCAL'))))
FIRST = bool(int(env.get("FIRST", cf.get('deploy', 'FIRST'))))
SECRET = env.get("SECRET", cf.get('deploy', 'SECRET'))
TOKEN = env.get("TOKEN", cf.get('deploy', 'TOKEN'))


# test
TEST_MYSQL_HOST = env.get("TEST_MYSQL_HOST", cf.get('test', 'TEST_MYSQL_HOST'))
TEST_MYSQL_PORT = int(env.get("TEST_MYSQL_PORT", cf.get('test', 'TEST_MYSQL_PORT')))
TEST_MYSQL_USER = env.get("TEST_MYSQL_USER", cf.get('test', 'TEST_MYSQL_USER'))
TEST_MYSQL_PASSWORD = env.get("TEST_MYSQL_PASSWORD", cf.get('test', 'TEST_MYSQL_PASSWORD'))
TEST_MYSQL_DB = env.get("TEST_MYSQL_DB", cf.get('test', 'TEST_MYSQL_DB'))

# 主题数据库
THEME_HOST = env.get("THEME_HOST", cf.get('theme', 'THEME_HOST'))
THEME_PORT = int(env.get("THEME_PORT", cf.get('theme', 'THEME_PORT')))
THEME_USER = env.get("THEME_USER", cf.get('theme', 'THEME_USER'))
THEME_PASSWD = env.get("THEME_PASSWD", cf.get('theme', 'THEME_PASSWD'))
THEME_DB = env.get("THEME_DB", cf.get('theme', 'THEME_DB'))

# 聚源
JUY_HOST = env.get("JUY_HOST", cf.get('juyuan', 'JUY_HOST'))
JUY_PORT = int(env.get("JUY_PORT", cf.get('juyuan', 'JUY_PORT')))
JUY_USER = env.get("JUY_USER", cf.get('juyuan', 'JUY_USER'))
JUY_PASSWD = env.get("JUY_PASSWD", cf.get('juyuan', 'JUY_PASSWD'))
JUY_DB = env.get("JUY_DB", cf.get('juyuan', 'JUY_DB'))

# 贝格
BG_HOST = env.get("BG_HOST", cf.get('bigdata', 'BG_HOST'))
BG_PORT = int(env.get("BG_PORT", cf.get('bigdata', 'BG_PORT')))
BG_USER = env.get("BG_USER", cf.get('bigdata', 'BG_USER'))
BG_PASSWD = env.get("JBG_PASSWD", cf.get('bigdata', 'BG_PASSWD'))
BG_DB = env.get("BG_DB", cf.get('bigdata', 'BG_DB'))

# api
LOCAL_API_HOST = env.get("LOCAL_API_HOST", cf.get('jzapi', 'LOCAL_API_HOST'))
REMOTE_API_HOST = env.get("REMOTE_API_HOST", cf.get('jzapi', 'REMOTE_API_HOST'))
API_HOST = LOCAL_API_HOST if LOCAL else REMOTE_API_HOST

AUTH_USERNAME = env.get("AUTH_USERNAME", cf.get('jzapi', 'AUTH_USERNAME'))
AUTH_PASSWORD = env.get("AUTH_PASSWORD", cf.get('jzapi', 'AUTH_PASSWORD'))

# spider
if LOCAL:
    SPIDER_MYSQL_HOST = TEST_MYSQL_HOST
    SPIDER_MYSQL_PORT = TEST_MYSQL_PORT
    SPIDER_MYSQL_USER = TEST_MYSQL_USER
    SPIDER_MYSQL_PASSWORD = TEST_MYSQL_PASSWORD
    SPIDER_MYSQL_DB = TEST_MYSQL_DB
else:
    SPIDER_MYSQL_HOST = env.get("SPIDER_MYSQL_HOST", cf.get('spider', 'SPIDER_MYSQL_HOST'))
    SPIDER_MYSQL_PORT = int(env.get("SPIDER_MYSQL_PORT", cf.get('spider', 'SPIDER_MYSQL_PORT')))
    SPIDER_MYSQL_USER = env.get("SPIDER_MYSQL_USER", cf.get('spider', 'SPIDER_MYSQL_USER'))
    SPIDER_MYSQL_PASSWORD = env.get("SPIDER_MYSQL_PASSWORD", cf.get('spider', 'SPIDER_MYSQL_PASSWORD'))
    SPIDER_MYSQL_DB = env.get("SPIDER_MYSQL_DB", cf.get('spider', 'SPIDER_MYSQL_DB'))


# product
if LOCAL:
    PRODUCT_MYSQL_HOST = TEST_MYSQL_HOST
    PRODUCT_MYSQL_PORT = TEST_MYSQL_PORT
    PRODUCT_MYSQL_USER = TEST_MYSQL_USER
    PRODUCT_MYSQL_PASSWORD = TEST_MYSQL_PASSWORD
    PRODUCT_MYSQL_DB = TEST_MYSQL_DB
else:
    PRODUCT_MYSQL_HOST = env.get("PRODUCT_MYSQL_HOST", cf.get('product', 'PRODUCT_MYSQL_HOST'))
    PRODUCT_MYSQL_PORT = env.get("PRODUCT_MYSQL_PORT", cf.get('product', 'PRODUCT_MYSQL_PORT'))
    PRODUCT_MYSQL_USER = env.get("PRODUCT_MYSQL_USER", cf.get('product', 'PRODUCT_MYSQL_USER'))
    PRODUCT_MYSQL_PASSWORD = env.get("PRODUCT_MYSQL_PASSWORD", cf.get('product', 'PRODUCT_MYSQL_PASSWORD'))
    PRODUCT_MYSQL_DB = env.get("PRODUCT_MYSQL_DB", cf.get('product', 'PRODUCT_MYSQL_DB'))


if __name__ == "__main__":
    import sys
    mod = sys.modules[__name__]
    attrs = dir(mod)
    attrs = [attr for attr in attrs if not attr.startswith("__") and attr.isupper()]
    for attr in attrs:
        print(attr, ":", getattr(mod, attr))
