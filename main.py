import time
from functools import partial
from multiprocessing import Pool, cpu_count, current_process
from os import makedirs, path

import pandas as pd

from exchangeConfig import *
from functions import *
from settings import *

# pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
# pd.set_option('display.max_rows', 5000)  # 最多显示数据的行数
pd.set_option("display.unicode.ambiguous_as_wide", True)
pd.set_option("display.unicode.east_asian_width", True)
logger = logging.getLogger("app.main")


def reporter(exchangeId, interval):
    process = current_process()
    process.name = "Reporter"
    while True:
        sendReport(exchangeId, interval)
        time.sleep(0.5)


def main():
    ex = getattr(ccxt, EXCHANGE)(EXCHANGE_CONFIG)

    # 开启一个非阻塞的报告进程
    rptpool = Pool(1)
    rptpool.apply_async(reporter, args=(EXCHANGE, REPORT_INTERVAL))
    
    # 开始运行策略
    while True:
        # 获取所有币种
        tickers = getTickers(exchange=ex)
        markets = getMarkets(ex)
        logger.info(f"获取到所有币种列表,共{len(tickers)}种")
        time.sleep(SLEEP_LONG)
        
        # 提取TOP币种List,合并黑白名单
        symbols = getTopN(tickers, rule=RULE, _type=TYPE, n=TOP)
        symbols = (set(symbols) | set(SYMBOLS_WHITE)) - set(SYMBOLS_BLACK)
        symbols = list(symbols)

        logger.info(f"获取到{TYPE} TOP{TOP}币种,\n白名单：{SYMBOLS_WHITE}\n黑名单：{SYMBOLS_BLACK}\n过滤后,共{len(symbols)}种:\n{symbols}")
        time.sleep(SLEEP_LONG)
        
        # 获取当前持仓情况
        openPosition = getOpenPosition(ex)
        # logger.debug(f"openPosition:\n{openPosition}")
        logger.info(f'获取到当前持仓情况:\n{openPosition[["notional", "unrealizedPnl", "leverage", "percentage", "entryPrice", "markPrice", "liquidationPrice", ]]}')
        time.sleep(SLEEP_LONG)

        # 获取各币种的历史k线,dict:{symbols: kline_df}
        timeStart = time.time()
        kHistory = getKlines(exchangeId=EXCHANGE, symbols=symbols, level=LEVEL, amount=PERIOD)
        # logger.debug(f"kHistory:\n{kHistory}")
        logger.info(f"获取到所有TOP币种历史k线,共{sum(map(len, kHistory.values()))}根")
        logger.debug(f"获取历史k线,用时{int(time.time()-timeStart)}秒")
        time.sleep(SLEEP_LONG)

        # 等待当前k线收盘
        sleepToClose(level=LEVEL, aheadSeconds=AHEAD_SEC, test=IS_TEST)

        # 并行获取轮动池的最新k线
        timeStart = time.time()
        singleGetKlines = partial(getKlines, EXCHANGE, LEVEL, NEW_KLINE_NUM)
        with Pool(processes=min(cpu_count(), len(symbols))) as pool:
            kNew = pool.map(singleGetKlines, [[symbol] for symbol in symbols])
        # 把kNew从一个字典的列表,转换成一个字典：[{'a': 1}, {'b': 2}]->{'a': 1, 'b': 2}
        kNew = {list(i.keys())[0]:list(i.values())[0] for i in kNew}
        # logger.debug(f"kNew:\n{kNew}")
        kAll = combineK(kHistory, kNew)
        logger.debug(f"并行取到所有TOP{TOP}币种最新k线,用时{int(time.time()-timeStart)}秒")
        # logger.debug(f"kAll:\n{kAll}")
        
        # 计算信号
        # 计算信号前再次更新持仓状态，避免中间被跟踪止损平掉仓位但是状态没更新为空，丢失信号的情况
        openPosition = getOpenPosition(ex)
        sig = getSignal2(kAll, openPosition=openPosition, factor=FACTOR, para=[PERIOD])
        logger.info(f"本周期计算信号完成：{sig}")

        # 根据信号下单
        if sig:
            logger.info(f"本周期出现交易信号,开始下单！")
            orderList = placeOrder2(ex, sig, markets)
            orderListStr = "\n".join(str(i) for i in orderList)
            sendAndPrintInfo(f"{STRATEGY_NAME}: 本周期出现交易信号:\n{sig}\n\n订单执行成功：\n{orderListStr}")
        # else:
        #     logger.info(f"所有币种因子均小于涨幅下限{MIN_CHANGE*100}%,本周期空仓。")
        #     orderList = closePosition(ex, openPosition)
        #     if orderList: sendAndPrintInfo(f"{STRATEGY_NAME}: 所有币种涨幅小于0,本周期空仓,清仓执行成功：\n{orderList}")


        # 下单后更新持仓状态,发送报告
        if IS_TEST:
            exit()
        time.sleep(SLEEP_LONG)




if __name__ == "__main__":
    main()
