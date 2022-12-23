import random
import pandas as pd

from logger import *

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 5000)  # 最多显示数据的行数
logger = logging.getLogger("app.sig")


# 所有signal都要输出一个带有chosen列的df，是选出的交易币种
# 传递给getPosition()，作为下一周期持仓依据

# 计算周期内的涨幅
# 例如20日涨幅
def signalMomentum(df, para=[20]):
    n = para[0]
    df["factor"] = df["close"].pct_change(periods=n)

    return df


def signalTest(df, para=[20]):
    n = para[0]
    df["factor"] = random.random() * -1

    return df