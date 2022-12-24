# 读取币种轮动池
# SYMBOLS = ["BTC", "ETH"]
SYMBOLS = ["BTC", "ETH", "DOGE", "BNB"]
# 轮动池文件
import os
DATA_PATH = "data"
SYMBOLS_FILE = os.path.join(DATA_PATH, "SYMBOLS.src")


# 选币算法
FACTOR = "signalMomentum"
# FACTOR = "signalTest"

# 选币参数
# 只选取USDT交易对的成交量top3
RULE = "/USDT"
TYPE = "quoteVolume"
TOP = 50
# 4h级别20根k线
LEVEL = "1h"
PERIOD = 20
# 杠杆倍数
LEVERAGE = 1
# 交易安全滑点
SLIPPAGE = 1 / 100
# 余额使用上限
MAX_BALANCE = 98 / 100


# 本轮开始之前的预留秒数，小于预留秒数则顺延至下轮
AHEAD_SEC = 3


# 获取最新k线的数量
NEW_KLINE_NUM = 5


# 输出目录
import datetime as dt
OUTPUT_PATH = "output"
TIME_PATH = dt.datetime.now().strftime("%Y-%m-%d_%H.%M.%S")


# 设置Log
import logging
LOG_PATH = "log"
LOG_LEVEL_CONSOLE = logging.DEBUG
LOG_LEVEL_FILE = logging.DEBUG


# 发送MIXIN报告
MIXIN_TOKEN = "mrbXSz6rSoQjtrVnDlOH9ogK8UubLdNKClUgx1kGjGoq39usdEzbHlwtFIvHHO3C"
# 报告发送间隔分钟
REPORT_INTERVAL = 1

# 休眠时间
SLEEP_SHORT = 0.5
SLEEP_MEDIUM = 3
SLEEP_LONG = 6


# 最大重试次数
MAX_TRY = 3
