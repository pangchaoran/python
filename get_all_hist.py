# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
print(datetime.now())

import tushare as ts
import pandas as pd
import numpy as np
import os

ts.set_token('1075d9a4eb51461266520905807f7d68806f68511ad0c90d938dc81af9ea6dee')

#get all stock code, name ticker,secShortName,exchangeCD,listDate
b01=ts.Master().SecID(assetClass='E',field='ticker,exchangeCD')
b01=b01[((b01['exchangeCD']=='XSHE') | (b01['exchangeCD']=='XSHG')) & (b01['ticker'].str.len()==6)].rename(columns={u'ticker':'code'})
b02=ts.get_stock_basics().loc[:,['name','outstanding','timeToMarket']]
b02['code']=b02.index.values
b0=pd.merge(b02,b01,how='left',on='code')

b1=list(b0.code)

#get Adj factor
adj0=pd.DataFrame()
for i in list(range(len(b1)))[::451]:
  adj1=ts.Market().MktAdjf(ticker=','.join(b1[i:min(i+450,len(b1))]),field='ticker,exDivDate,adjFactor')
  if adj0.empty:
    adj0=adj1
  else:
    adj0=pd.concat([adj0,adj1])

adj0=adj0.rename(columns={u'ticker':'code',u'exDivDate':'date'})
adj0['code']=adj0['code'].map(lambda x: str(x).zfill(6))


#if csv file not exist, then get all history data
if not [x for x in os.listdir('./rawdata') if os.path.splitext(x)[1]=='.csv']:
  h0=pd.DataFrame()
  for code in b1:
    h1=ts.get_hist_data(code)
    #h1=h1.sort_index(axis=0)
    #h1['ma51']=pd.rolling_mean(h1['close'],window=5)
    if h1 is not None:
      h1['code']=code
      if h0.empty:
        h0=h1
      else:
        h0=pd.concat([h0,h1])
  d1=max(list(h0.index.values))
  h0['date']=h0.index.values
  h0=pd.merge(h0,adj0,how='left',on=['code','date'])
  h0=pd.merge(h0,b0,how='left',on='code').set_index('date')
  h0.to_csv('./rawdata/'+d1+'.csv')
else:
  d1=[x for x in os.listdir('./rawdata') if os.path.splitext(x)[1]=='.csv'][0].replace('.csv','')
  d2=(datetime.strptime(d1,'%Y-%m-%d')+timedelta(days=1)).strftime('%Y-%m-%d')
  
  #get the latest data
  t0=pd.DataFrame()
  for code in b1:
    t1=ts.get_hist_data(code,start=d2)
    if t1 is not None:
      t1['code']=code
      if t0.empty:
        t0=t1
      else:
        t0=pd.concat([t0,t1])
  t0['date']=t0.index.values
  t0=pd.merge(t0,adj0,how='left',on=['code','date'])
  t0=pd.merge(t0,b0,how='left',on='code').set_index('date')
  
  #append if latest data exists
  if not t0.empty:
    d3=max(list(t0.index.values))
    os.rename('./rawdata/'+d1+'.csv','./rawdata/'+d3+'.csv')
    t0.to_csv('./rawdata/'+d3+'.csv',mode='a',header=None)







     
print(datetime.now())

