# 策略命名
STRATEGY_NAME = "鹤"
IS_TEST = False

# 轮动池黑白名单,格式"BTC/USDT"
SYMBOLS_WHITE = []
SYMBOLS_BLACK = ["BTCDOM/USDT", "DEFI/USDT", "BLUEBIRD/USDT"]


# 选币算法
FACTOR = "signalMomentum"
# FACTOR = "signalTest"

# 选币范围
# 只选取USDT交易对的成交量topN
RULE = "/USDT"
TYPE = "quoteVolume"
TOP = 200
# 4h级别20根k线
LEVEL = "4h"
PERIOD = 3
# 选币个数
SELECTION_NUM = 2
# 跟踪止盈开关，跟踪比例，注意TP和SL的比例都是价格比例，
# 如果价格波动1%，2倍杠杆，那么ROE的波动就是2%
ENABLE_TP = True
TP_PERCENT = 3/100
# 止损开关，开仓价的向下比例
ENABLE_SL = True
SL_PERCENT = 3/100
# 涨幅下限，小于此空仓
MIN_CHANGE = 0 / 100
# 杠杆倍数
LEVERAGE = 1
# 交易安全滑点
SLIPPAGE = 1 / 100
# 余额使用上限
MAX_BALANCE = 99 / 100


# 本轮开始之前的预留秒数，小于预留秒数则顺延至下轮
AHEAD_SEC = 3
# 向前偏移秒数，为了避免在同一时间下单造成踩踏
OFFSET_SEC = 0

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
# 报告发送间隔分钟
REPORT_INTERVAL = 30

# 休眠时间
SLEEP_SHORT = 0.5
SLEEP_MEDIUM = 3
SLEEP_LONG = 6


# 最大重试次数
MAX_TRY = 3
