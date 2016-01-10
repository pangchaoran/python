# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
print(datetime.now())

import tushare as ts
import pandas as pd
import numpy as np
import os

ts.set_token('1075d9a4eb51461266520905807f7d68806f68511ad0c90d938dc81af9ea6dee')

ts.Master().TradeCal(exchangeCD='XSHG,XSHE',beginDate='20130101').to_csv('TradeCal.csv')


