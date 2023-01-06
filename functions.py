import datetime as dt
import time
import math
from functools import reduce

import ccxt
import numpy as np
import pandas as pd
import requests
from tenacity import *

import signals
from exchangeConfig import *
from logger import *
from settings import *

# pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
# pd.set_option('display.max_rows', 5000)  # 最多显示数据的行数
pd.set_option("display.unicode.ambiguous_as_wide", True)
pd.set_option("display.unicode.east_asian_width", True)
logger = logging.getLogger("app.func")


@retry(stop=stop_after_attempt(3), wait=wait_fixed(SLEEP_SHORT), reraise=True,
        before_sleep=before_sleep_log(logger, logging.ERROR),
        )
def callAlarm(strategyName=STRATEGY_NAME, content="存在严重风险项，请立即检查"):
    url = "http://api.aiops.com/alert/api/event"
    apiKey = "66e6aeab4218431f8afe7e76ac96c38e"
    eventId = str(int(time.time()))
    stragetyName = strategyName
    content = content
    para = f"?app={apiKey}&eventType=trigger&eventId={eventId}&priority=3&host={stragetyName}&alarmContent={content}"

    try:
        r = requests.post(url+para)
        if r.json()["result"] != "success":
            sendAndPrintError(f"电话告警触发失败，可能有严重风险，请立即检查！{r.text}")
    except Exception as e:
        logger.error(f"电话告警触发失败，可能有严重风险，请立即检查！{e}")
        logger.exception(e)


@retry(stop=stop_after_attempt(3), wait=wait_fixed(SLEEP_SHORT), reraise=True,
        before_sleep=before_sleep_log(logger, logging.ERROR),
        )
def sendMixin(msg, _type="PLAIN_TEXT"):
    token = MIXIN_TOKEN
    url = f"https://webhook.exinwork.com/api/send?access_token={token}"

    value = {
        'category': _type,
        'data': msg,
        }
    
    try:
        r = requests.post(url, data=value, timeout=2).json()
    except Exception as err:
        logger.exception(err)


def sendAndPrintInfo(msg):
    logger.info(msg)
    sendMixin(msg)


def sendAndPrintError(msg):
    logger.error(msg)
    sendMixin(msg)


def sendAndCritical(msg):
    logger.critical(msg)
    callAlarm(strategyName=STRATEGY_NAME, content=msg)
    sendMixin(msg)


def sendAndRaise(msg):
    logger.error(msg)
    sendMixin(msg)
    raise RuntimeError(msg)


def sendReport(exchangeId, interval=REPORT_INTERVAL):
    exchange = getattr(ccxt, exchangeId)(EXCHANGE_CONFIG)

    nowMinute = dt.datetime.now().minute
    nowSecond = dt.datetime.now().second

    if (nowMinute%interval==0) and (nowSecond==59):
        logger.debug("开始发送报告")

        pos = getOpenPosition(exchange)
        bTot, bBal, bPos = getBalances(exchange)
        bal = round(float(bTot.iloc[0]["availableBalance"]),2)

        msg = f"### {STRATEGY_NAME} - 策略报告\n\n"

        if pos.shape[0] > 0:
            pos = pos[[
                "notional",
                "percentage",
                "unrealizedPnl",
                "markPrice",
                "liquidationPrice",
                "datetime",
                "leverage",
            ]]
            pos.rename(columns={
                "notional": "持仓价值(U)",
                "percentage": "盈亏比例(%)",
                "unrealizedPnl": "未实现盈亏(U)",
                "markPrice": "标记价格(U)",
                "liquidationPrice": "爆仓价格(U)",
                "datetime": "开仓时间",
                "leverage": "杠杆倍数",
            }, inplace=True)
            d = pos.to_dict(orient="index")

            msg += f'#### 当前持币 : {", ".join(list(d.keys()))}'
            for k,v in d.items():
                msg += f"""
#### {k}
 - 持仓价值(U) : {v["持仓价值(U)"]}
 - 盈亏比例(%) : {v["盈亏比例(%)"]}
 - 未实现盈亏(U) : {v["未实现盈亏(U)"]}
 - 标记价格(U) : {v["标记价格(U)"]}
 - 爆仓价格(U) : {v["爆仓价格(U)"]}
 - 开仓时间 : {v["开仓时间"]}
 - 杠杆倍数 : {v["杠杆倍数"]}
"""

        else:
            msg += "#### 当前空仓\n"
        
        msg += f"#### 轮动数量 : {TOP+len(SYMBOLS_WHITE)-len(SYMBOLS_BLACK)}\n"
        msg += f"#### 策略级别 : {LEVEL}\n"
        msg += f"#### 策略周期 : {PERIOD}\n"
        msg += f"#### 跟踪止损 : {TP_PERCENT if ENABLE_TP else 'False'}\n"
        msg += f"#### 账户余额 : {bal}U\n"
        msg += f"#### 使用上限 : {MAX_BALANCE*100}%\n"

        sendMixin(msg, _type="PLAIN_POST")


def secondsToNext(exchange, level):
    levelSeconds = exchange.parseTimeframe(level.lower())
    now = int(time.time())
    seconds = levelSeconds - (now % levelSeconds)
    
    return seconds


def nextStartTime(level, ahead_seconds=3):
    # ahead_seconds为预留秒数，
    # 当离开始时间太近，本轮可能来不及下单，因此当离开始时间的秒数小于预留秒数时，
    # 就直接顺延至下一轮开始
    if level.endswith('m') or level.endswith('h'):
        pass
    elif level.endswith('T'):
        level = level.replace('T', 'm')
    elif level.endswith('H'):
        level = level.replace('H', 'h')
    else:
        sendAndRaise(f"{STRATEGY_NAME}: level格式错误。程序退出。")

    ti = pd.to_timedelta(level)
    now_time = dt.datetime.now()
    # now_time = dt.datetime(2019, 5, 9, 23, 50, 30)  # 修改now_time，可用于测试
    this_midnight = now_time.replace(hour=0, minute=0, second=0, microsecond=0)
    min_step = dt.timedelta(minutes=1)

    target_time = now_time.replace(second=0, microsecond=0)

    while True:
        target_time = target_time + min_step
        delta = target_time - this_midnight
        if delta.seconds % ti.seconds == 0 and (target_time - now_time).seconds >= ahead_seconds:
            # 当符合运行周期，并且目标时间有足够大的余地，默认为60s
            break

    return target_time


def sleepToClose(level, aheadSeconds, test=False):
    nextTime = nextStartTime(level, ahead_seconds=aheadSeconds)
    logger.info(f"等待当前k线收盘，新k线开始时间 {nextTime}")
    if test is False:
        time.sleep(max(0, (nextTime - dt.datetime.now()).seconds))
        while True:  # 在靠近目标时间时
            if dt.datetime.now() > nextTime:
                break
    logger.info(f"新k线开盘，开始计算信号！")


@retry(stop=stop_after_attempt(3), wait=wait_fixed(SLEEP_SHORT), reraise=True,
        before_sleep=before_sleep_log(logger, logging.ERROR))
def getMarkets(exchange):
    try:
        mks = exchange.loadMarkets()
        mks = pd.DataFrame.from_dict(mks, orient="index")
        return mks
    except Exception as e:
        logger.exception(e)
        sendAndRaise(f"{STRATEGY_NAME}: getMarkets()错误，程序退出。{e}")


@retry(stop=stop_after_attempt(3), wait=wait_fixed(SLEEP_SHORT), reraise=True,
        before_sleep=before_sleep_log(logger, logging.ERROR))
def getTickers(exchange):
    try:
        tk = exchange.fetchTickers()
        tk = pd.DataFrame.from_dict(tk, orient="index")
        return tk
    except Exception as e:
        logger.exception(e)
        sendAndRaise(f"{STRATEGY_NAME}: getTickers()错误，程序退出。{e}")


@retry(stop=stop_after_attempt(3), wait=wait_fixed(SLEEP_SHORT), reraise=True,
        before_sleep=before_sleep_log(logger, logging.ERROR))
def getTicker(exchange, symbol):
    try:
        tk = exchange.fetchTicker(symbol)
        tk = pd.DataFrame(tk, index=[0])
        return tk
    except Exception as e:
        logger.exception(e)
        sendAndRaise(f"{STRATEGY_NAME}: getTicker()错误，程序退出。{e}")


def getTopN(tickers, rule="/USDT", _type="quoteVolume", n=50):
    tickers["timestamp"] = pd.to_datetime(tickers["timestamp"], unit="ms")
    tickers = tickers.filter(like=rule, axis=0)
    r = tickers.set_index("timestamp").sort_index().last("24h").nlargest(n, _type)["symbol"]
    return list(r)


@retry(stop=stop_after_attempt(3), wait=wait_fixed(SLEEP_SHORT), reraise=True,
        before_sleep=before_sleep_log(logger, logging.ERROR))
def getBalances(exchange):
    # positions:
    # initialMargin maintMargin unrealizedProfit positionInitialMargin openOrderInitialMargin leverage  isolated entryPrice maxNotional positionSide positionAmt notional isolatedWallet updateTime bidNotional askNotional
    try:
        b = exchange.fetchBalance()["info"]
        balances = pd.DataFrame(b["assets"])
        balances.set_index("asset", inplace=True)
        balances.index.name = None
        positions = pd.DataFrame(b["positions"])
        positions.set_index("symbol", inplace=True)
        positions.index.name = None
        b.pop("assets")
        b.pop("positions")
        total = pd.DataFrame(b, index=[0])
        return total, balances, positions
    except Exception as e:
        logger.exception(e)
        sendAndRaise(f"{STRATEGY_NAME}: getBalances()错误，程序退出。{e}")


@retry(stop=stop_after_attempt(3), wait=wait_fixed(SLEEP_SHORT), reraise=True,
        before_sleep=before_sleep_log(logger, logging.ERROR))
def getKlines(exchangeId, level, amount, symbols):
    # getKlines要在子进程里使用，进程之间不能直接传递ccxt实例，因此只能在进程内部创建实例
    exchange = getattr(ccxt, exchangeId)(EXCHANGE_CONFIG)
    amount += 10
    klines = dict.fromkeys(symbols, None)

    for symbol in symbols:
        k = exchange.fetchOHLCV(symbol, level, limit=amount)
        k = pd.DataFrame(k, columns=[
            "candle_begin_time",
            "open",
            "high",
            "low",
            "close",
            "volume"
        ])
        k.drop_duplicates(subset=["candle_begin_time"], keep="last", inplace=True)
        k.sort_values(by="candle_begin_time", inplace=True)
        k["candle_begin_time"] = pd.to_datetime(k["candle_begin_time"], unit="ms") + dt.timedelta(hours=8)
        k = k[:-1]
        klines[symbol] = k
        logger.debug(f"获取到{symbol} k线{len(k)}根")

        if len(symbols)>1: time.sleep(SLEEP_SHORT)

    return klines


def combineK(kHistory, kNew):
    if kHistory.keys() != kNew.keys():
        sendAndRaise(f"{STRATEGY_NAME}: combineK()报错：历史k线与最新k线的symbols不一致，请检查。退出。")
    
    kAll = dict.fromkeys(kHistory.keys())
    for symbol in kHistory.keys():
        kAll[symbol] = pd.concat([kHistory[symbol], kNew[symbol]], ignore_index=True)
        kAll[symbol].drop_duplicates(subset="candle_begin_time", keep="last", inplace=True)
        kAll[symbol].sort_values(by="candle_begin_time", inplace=True)
        kAll[symbol].reset_index(drop=True, inplace=True)
    
    return kAll


@retry(stop=stop_after_attempt(3), wait=wait_fixed(SLEEP_SHORT), reraise=True,
        before_sleep=before_sleep_log(logger, logging.ERROR))
def getPositions(exchange):
    # positions:
    # info    id  contracts  contractSize  unrealizedPnl  leverage liquidationPrice  collateral  notional markPrice  entryPrice timestamp  initialMargin  initialMarginPercentage  maintenanceMargin  maintenanceMarginPercentage marginRatio datetime marginMode marginType  side  hedged percentage
    try:
        p = exchange.fetchPositions()
        p = pd.DataFrame(p)
        p.set_index("symbol", inplace=True)
        p.index.name = None
        return p
    except Exception as e:
        logger.exception(e)
        sendAndRaise(f"{STRATEGY_NAME}: getPositions()错误，程序退出。{e}")


def getOpenPosition(exchange):
    pos = getPositions(exchange)
    op = pos.loc[pos["contracts"]!=0]
    return op


def getSignal(klines, openPosition, factor, para):
    # 每个币种计算因子列
    for symbol,df in klines.items():
        # 计算因子列
        df = getattr(signals, factor)(df, para)

        df.rename(columns={
            "open": f"{symbol}_open",
            "high": f"{symbol}_high",
            "low": f"{symbol}_low",
            "close": f"{symbol}_close",
            "volume": f"{symbol}_volume",
            "factor": f"{symbol}_factor",
        }, inplace=True)
    
    # 汇总每个币种的df，生成总的dfAll
    dfs = list(klines.values())
    dfAll = reduce(lambda df1,df2: pd.merge(df1,df2, on="candle_begin_time"), dfs)

    dfAll.drop_duplicates(subset="candle_begin_time", ignore_index=True, inplace=True)
    dfAll.sort_values(by="candle_begin_time", inplace=True)
    dfAll.reset_index(drop=True, inplace=True)

    # 根据factor选币
    # 如果最大涨幅都小于0，那么空仓
    if dfAll.iloc[-1].filter(like="factor").max() < MIN_CHANGE: return 0

    # .idxmax(axis=1)选取一行中的最大值的列名，即选取最大factor的币种
    # 列名如ETH_factor，用replace把_factor去掉
    dfAll["chosen"] = dfAll.filter(like="factor").idxmax(axis=1).str.replace("_factor","")
    logger.debug(f"dfAllWithChosen:\n{dfAll.iloc[-1].filter(regex='.*factor|chosen')}")
    
    # 根据现有持仓，生成交易信号
    has = openPosition.index.tolist()[0] if len(openPosition) else None
    logger.debug(f"has: {has}")
    new = dfAll.iloc[-1]["chosen"]
    logger.debug(f"new: {new}")

    if has != new:
        sig = {
            0: has,
            1: new,
        }
    else:
        sig = None

    return sig


def getSignal2(klines, openPosition, factor, para):
    
    # 每个币种计算因子列
    for symbol,df in klines.items():
        logger.debug(symbol)
        # 计算因子列
        df = getattr(signals, factor)(df, para)
        df["symbol"] = symbol

    # 汇总每个币种的df，生成总的dfAll
    dfs = list(klines.values())
    dfAll = reduce(lambda df1,df2: pd.concat([df1,df2], ignore_index=True), dfs)
    # 根据时间和因子排序，最新k线的因子排序出现在最后
    g = dfAll.groupby("candle_begin_time")
    # 有些币缺少k线，会导致最后一组k线的数量变少，因此用最后一组k线的数量作为选币池的总个数，过滤掉最后一组中没有出现的币种
    coins_num = g.size()[-1]
    logger.debug(f"币池总数{len(klines)}, 最新k线币总数{coins_num}")
    dfAll["rank"] = g["factor"].rank(ascending=False, method="first")
    dfAll.sort_values(by=["candle_begin_time", "rank"], inplace=True)
    # 最新k线的排序结果
    dfNew = dfAll.tail(coins_num)
    logger.info(f'本周期因子排序结果:\n{dfNew[["candle_begin_time", "symbol", "factor", "rank"]]}')

    # 根据因子排序选前几名的币，也可以选后几名的币
    longCoins = dfNew.head(min(SELECTION_NUM, int(len(dfNew)/2)))
    # shortCoins = dfNew.tail(min(SELECTION_NUM, int(len(dfNew)/2)))
    # 还要满足下限参数的要求
    longCoins = longCoins.loc[dfNew["factor"]>MIN_CHANGE]
    # 最后得出一个symbol list
    longCoins = longCoins["symbol"].values.tolist()
    
    # 根据现有持仓，生成交易信号
    has = openPosition.index.tolist()
    logger.debug(f"has: {has}")
    new = longCoins
    logger.debug(f"new: {new}")

    if set(has) != set(new):
        sig = {
            0: [i for i in has if i not in new],
            1: list(set(new) - set(has)),
        }
    else:
        sig = None

    return sig


@retry(stop=stop_after_attempt(3), wait=wait_fixed(SLEEP_SHORT), reraise=True,
        before_sleep=before_sleep_log(logger, logging.ERROR))
def setMarginType(exchange, symbolId, _type=1):
    if _type==1:
        t = "CROSSED"
    elif _type==2:
        t = "ISOLATED"
    
    p = {
        "symbol": symbolId,
        "marginType": t,
    }

    try:
        exchange.fapiPrivatePostMargintype(p)
    except ccxt.MarginModeAlreadySet:
        pass


def getOrderPrice(symbol, price, action, markets):
    precision = markets.loc[symbol, "precision"]["price"]
    
    if action == 1:
        orderPrice = price * (1 + SLIPPAGE)
    elif action == 0:
        orderPrice = price * (1 - SLIPPAGE)
    
    orderPrice = int(orderPrice * (10**precision)) / (10**precision)
    # orderPrice = exchange.priceToPrecision(symbol, orderPrice)
    logger.debug(f"symbol:{symbol}, slippage:{SLIPPAGE}, price:{price}, pre:{precision}, oP:{orderPrice}")
    
    return orderPrice


def getOrderSize(symbol, action, price, balance, markets, positions):
    # 如果是卖单则直接返回现有持仓数量
    if action==0: return abs(float(positions.loc[markets.loc[symbol, "id"], "positionAmt"]))

    # 如果是买单，则根据余额计算数量
    precision = markets.loc[symbol, "precision"]["amount"]
    minLimit = markets.loc[symbol, "limits"]["amount"]["min"]
    maxLimit = markets.loc[symbol, "limits"]["market"]["max"]

    size = max(balance*LEVERAGE/price, minLimit)
    size = min(size, maxLimit)
    size = int(size * (10**precision)) / (10**precision)
    logger.debug(f"symbol:{symbol}, maxBalance:{MAX_BALANCE}, price:{price}, pre:{precision}, size:{size}, min:{round(0.1**precision, precision)}, minLimit:{minLimit}, maxLimit:{maxLimit}")
    if precision==0:
        size = int(size)
        return max(size, minLimit)
    else:
        return max(size, round(0.1**precision, precision))


@retry(stop=stop_after_attempt(3), wait=wait_fixed(SLEEP_SHORT), reraise=True,
        before_sleep=before_sleep_log(logger, logging.ERROR))
def getOrderStatus(exchange, symbolId, orderId):
    return exchange.fapiPrivateGetOrder({
        "symbol": symbolId,
        "orderId": orderId,
    })["status"]


@retry(stop=stop_after_attempt(3), wait=wait_fixed(SLEEP_SHORT), reraise=True,
        before_sleep=before_sleep_log(logger, logging.ERROR))                  
def placeOrder(exchange, signal, markets):

    try:
        setMarginType(exchange, markets.loc[signal[1], "id"], _type=1)
        exchange.setLeverage(LEVERAGE, signal[1])
    except Exception as e:
        pass

    orderList = []
    # sorted(signal)的结果是[0,1]，即先执行卖单再执行买单
    for action in sorted(signal):
        logger.debug(f"action:{action}, symbol:{signal[action]}")
        # 如果现有持仓为空则跳过
        if signal[action] is None: continue

        bTotal, bBalances, bPositions = getBalances(exchange)
        balance = float(bTotal.iloc[0]["availableBalance"])
        price = float(getTicker(exchange, signal[action]).iloc[0]["last"])
        price = getOrderPrice(signal[action], price, action, markets)
        quantity = getOrderSize(signal[action], action, price, balance, markets, bPositions)
        if action==0 and quantity == 0:
            logger.info(f"平仓之前持仓已经为0，可能已经被跟踪止损，不平仓。")
            continue
        
        p = {
            "symbol": markets.loc[signal[action], "id"],
            "side": "BUY" if action ==1 else "SELL",
            # "positionSide": "LONG",
            "type": "LIMIT",
            "price": price,
            "quantity": quantity,
            "newClientOrderId": f"Rock{exchange.milliseconds()}",
            "timeInForce": "GTC",  # 必须参数"有效方式":GTC - Good Till Cancel 成交为止
        }


        logger.debug(f"placeOrder()本次下单参数: {p}")

        try:
            orderInfo = exchange.fapiPrivatePostOrder(p)
            orderId = orderInfo["orderId"]

            time.sleep(SLEEP_SHORT)
                
            for i in range(MAX_TRY):
                orderStatue = exchange.fapiPrivateGetOrder({
                    "symbol": markets.loc[signal[action], "id"],
                    "orderId": orderId,
                })
                if orderStatue["status"] == "FILLED":
                    orderList.append(orderStatue)
                    logger.debug(f"placeOrder()订单成交：{orderStatue}")
                    break
                else:
                    if i == MAX_TRY - 1:
                        sendAndPrintError(f"{STRATEGY_NAME}: placeOrder()订单状态一直未成交FILLED,程序不退出,请尽快检查。")
                    time.sleep(SLEEP_SHORT)

        except Exception as e:
            sendAndPrintError(f"{STRATEGY_NAME}: placeOrder()下单出错。程序不退出。请检查: {e}")
            logger.exception(e)

        # 如果是平仓单（卖单），还需要撤销与之关联的所有挂单，比如跟踪止损单
        if action==0:
            try:
                p = {
                    "symbol": markets.loc[signal[action], "id"],
                }
                logger.debug(f"撤销所有订单: {p}")
                exchange.fapiPrivateDeleteAllopenorders(p)
            except Exception as e:
                sendAndPrintError(f"{STRATEGY_NAME}: placeOrder()撤销所有关联挂单失败。程序不退出。请检查: {e}")
                logger.exception(e)

    # 下跟踪止盈单
    if ENABLE_TP:
        try:
            symbol = signal[1]
            symbolId = markets.loc[symbol, "id"]
            r = exchange.fetchPositions([symbol])
            quantity = r[0]["contracts"]  # one-way mode单向持仓模式时
            # quantity = r[1]["contracts"]  # hedge mode双向持仓模式时

            tpPara = {
                "symbol": symbolId,
                "side": "SELL",
                "type": "TRAILING_STOP_MARKET",
                "quantity": quantity,
                "callbackRate": TP_PERCENT*100,
                "reduceOnly": True,
            }
            logger.debug(f"{symbol}跟踪止损单参数:{tpPara}")
            exchange.fapiPrivatePostOrder(tpPara)
        except Exception as e:
            sendAndPrintError(f"{STRATEGY_NAME}: 重大风险!placeOrder({symbol})跟踪止盈下单失败。程序不退出。请检查日志: {e}")
            logger.exception(e)
    
    return orderList


@retry(stop=stop_after_attempt(3), wait=wait_fixed(SLEEP_SHORT), reraise=True,
        before_sleep=before_sleep_log(logger, logging.ERROR))                  
def placeOrder2(exchange, signal, markets):
    orderList = []
    
    # 执行卖出
    if signal[0]:
        pos = getOpenPosition(exchange)
        for s in signal[0]:
            if s not in pos.index:
                logger.info(f"placeOrder({s})平仓之前已经没有持仓,可能已经被跟踪止损,本次不再平仓。")
                continue
            price = float(getTicker(exchange, s).iloc[0]["last"])
            price = getOrderPrice(symbol=s, price=price, action=0, markets=markets)
            quantity = abs(float(pos.loc[s, "contracts"]))
            
            p = {
                "symbol": markets.loc[s, "id"],
                "side": "SELL",
                "type": "LIMIT",
                "price": price,
                "quantity": quantity,
                "newClientOrderId": f"Rock{exchange.milliseconds()}",
                "timeInForce": "GTC",  # 必须参数"有效方式":GTC - Good Till Cancel 成交为止
                "reduceOnly": True,
            }

            logger.debug(f"placeOrder({s})平仓单参数: {p}")
            try:
                orderInfo = exchange.fapiPrivatePostOrder(p)
                orderId = orderInfo["orderId"]

                time.sleep(SLEEP_SHORT)
                    
                for i in range(MAX_TRY):
                    orderStatue = exchange.fapiPrivateGetOrder({
                        "symbol": markets.loc[s, "id"],
                        "orderId": orderId,
                    })
                    if orderStatue["status"] == "FILLED":
                        orderList.append([orderStatue["symbol"], orderStatue["side"], orderStatue["status"]])
                        logger.info(f"placeOrder({s})平仓单成交：{orderStatue}")
                        break
                    else:
                        if i == MAX_TRY - 1:
                            sendAndCritical(f"{STRATEGY_NAME}: placeOrder({s})平仓单一直未成交,程序不退出,请尽快检查。")
                        time.sleep(SLEEP_SHORT)
                
                # 平仓后撤销跟踪止盈订单，避免影响后续再开仓的同币订单
                try:
                    p = {
                        "symbol": markets.loc[s, "id"],
                    }
                    logger.info(f"placeOrder({s})平仓后撤销所有关联挂单: {p}")
                    exchange.fapiPrivateDeleteAllopenorders(p)
                except Exception as e:
                    sendAndCritical(f"{STRATEGY_NAME}: placeOrder({s})撤销所有关联挂单失败。程序不退出。请检查: {e}")
                    logger.exception(e)

            except Exception as e:
                sendAndCritical(f"{STRATEGY_NAME}: placeOrder({s})平仓单下单出错。程序不退出。请检查: {e}")
                logger.exception(e)


    # 执行买入
    if signal[1]:
        bTotal, bBalances, bPositions = getBalances(exchange)
        balance = float(bTotal.iloc[0]["availableBalance"]) * MAX_BALANCE
        for s in signal[1]:
            try:
                setMarginType(exchange, markets.loc[s, "id"], _type=1)
                exchange.setLeverage(LEVERAGE, s)
            except Exception as e:
                pass

            balanceForMe = balance / len(signal[1])
            logger.debug(f"{s}本次使用余额{balanceForMe}")
            price = float(getTicker(exchange, s).iloc[0]["last"])
            price = getOrderPrice(symbol=s, price=price, action=1, markets=markets)
            quantity = getOrderSize(symbol=s, action=1, price=price, balance=balanceForMe, markets=markets, positions=bPositions)
            p = {
                "symbol": markets.loc[s, "id"],
                "side": "BUY",
                "type": "LIMIT",
                "price": price,
                "quantity": quantity,
                "newClientOrderId": f"Rock{exchange.milliseconds()}",
                "timeInForce": "GTC",  # 必须参数"有效方式":GTC - Good Till Cancel 成交为止
            }

            logger.debug(f"placeOrder({s})开仓单参数: {p}")
            try:
                orderInfo = exchange.fapiPrivatePostOrder(p)
                orderId = orderInfo["orderId"]

                time.sleep(SLEEP_SHORT)
                    
                for i in range(MAX_TRY):
                    orderStatue = exchange.fapiPrivateGetOrder({
                        "symbol": markets.loc[s, "id"],
                        "orderId": orderId,
                    })
                    if orderStatue["status"] == "FILLED":
                        orderList.append([orderStatue["symbol"], orderStatue["side"], orderStatue["status"]])
                        logger.info(f"placeOrder({s})开仓单成交：{orderStatue}")
                        break
                    else:
                        if i == MAX_TRY - 1:
                            sendAndPrintError(f"{STRATEGY_NAME}: placeOrder({s})开仓单一直未成交,程序不退出,请尽快检查。")
                        time.sleep(SLEEP_SHORT)

            except Exception as e:
                sendAndPrintError(f"{STRATEGY_NAME}: placeOrder({s})开仓单下单出错。程序不退出。请检查: {e}")
                logger.exception(e)

            # 开仓成功后，下跟踪止盈单
            if ENABLE_TP:
                try:
                    symbolId = markets.loc[s, "id"]
                    r = exchange.fetchPositions([s])
                    quantityTotal = r[0]["contracts"]  # one-way mode单向持仓模式时
                    # quantityTotal = r[1]["contracts"]  # hedge mode双向持仓模式时
                    
                    # 跟踪止损单是市价单，市价单的最大下单限制比较小，需要考虑拆分下单
                    maxLimit = markets.loc[s, "limits"]["market"]["max"]
                    for i in range(math.ceil(quantityTotal/maxLimit)):
                        tpPara = {
                            "symbol": symbolId,
                            "side": "SELL",
                            "type": "TRAILING_STOP_MARKET",
                            "quantity": min(quantityTotal, maxLimit),
                            "callbackRate": TP_PERCENT*100,
                            "reduceOnly": True,
                        }
                        logger.debug(f"{s}跟踪止损订单参数:{tpPara}")
                        exchange.fapiPrivatePostOrder(tpPara)
                except Exception as e:
                    sendAndCritical(f"{STRATEGY_NAME}: placeOrder({s})跟踪止盈下单失败。程序不退出。请检查日志: {e}")
                    logger.exception(e)
                

    
    return orderList


def closePosition(exchange, openPositions):
    orderList = []
    if len(openPositions) > 0:
        
        p = {
            "symbol": openPositions.iloc[0]["info"]["symbol"],
            "side": "SELL",
            # "positionSide": "LONG",
            "type": "MARKET",
            # "price": getOrderPrice(signal[action], price, action, markets),
            "quantity": openPositions.iloc[0]["contracts"],
            "newClientOrderId": f"Rock{exchange.milliseconds()}",
            # "timeInForce": "GTC",  # 必须参数"有效方式":GTC - Good Till Cancel 成交为止
        }

        try:
            orderInfo = exchange.fapiPrivatePostOrder(p)
            orderId = orderInfo["orderId"]
        except Exception as e:
            logger.exception(e)
            sendAndCritical(f"{STRATEGY_NAME}: closePosition()平仓出错，请检查。{e}")
        
        for i in range(MAX_TRY):
            orderStatue = exchange.fapiPrivateGetOrder({
                "symbol": openPositions.iloc[0]["info"]["symbol"],
                "orderId": orderId,
            })
            if orderStatue["status"] == "FILLED":
                orderList.append(orderStatue)
                logger.debug(f"closePosition()订单成交：{orderStatue}")
                break
            else:
                if i == MAX_TRY - 1:
                    sendAndCritical(f"{STRATEGY_NAME}: closePosition()平仓一直未成交FILLED,程序不退出,请尽快检查。")
                time.sleep(SLEEP_SHORT)

    return orderList




if __name__ == "__main__":
    ## for test only
    ex = ccxt.binance(EXCHANGE_CONFIG)
    a,b,c = getBalances(ex)
    print(a)
    print(b)
    print(c)
