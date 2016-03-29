# -*- coding:utf-8 -*-

import tushare as ts
import pandas as pd
import os
import datetime
import lxml
from multiprocessing.dummy import Pool as ThreadPool
import pymongo
from pymongo import MongoClient
import json


TABLE_STOCKS_BASIC = 'stock_basic_list'
DOWNLOADDIR = os.path.pardir + '/stockdata/' # os.path.pardir: 上级目录
DOWNLOADDIR = 'd:/stockdata/' # os.path.pardir: 上级目录
STOCK_LIST=pd.DataFrame.from_csv(DOWNLOADDIR + TABLE_STOCKS_BASIC + '.csv')
#df = pd.DataFrame.from_csv(path=DOWNLOADDIR+'h_kline_000001.csv')

conn = MongoClient('localhost', 27017)
db = conn.fin 

# 补全股票代码(6位股票代码)
# input: int or string
# output: string
def getSixDigitalStockCode(code):
    strZero = ''
    for i in range(len(str(code)), 6):
        strZero += '0'
    return strZero + str(code)

def download_stock_basic_info():
    try:
        df = ts.get_stock_basics()
        #直接保存到csv
        print 'choose csv'
        df.to_csv(DOWNLOADDIR + TABLE_STOCKS_BASIC + '.csv');
        print 'download csv finish'
    except Exception as e:
        print str(e)        

def download_stock_kline(code0, date_start='', date_end=datetime.date.today()):

    code = getSixDigitalStockCode(code0) # 将股票代码格式化为6位数字
    #print code

    try:
        fileName = 'h_kline_' + str(code) + '.csv'

        writeMode = 'w'
        if os.path.exists(DOWNLOADDIR+fileName):
            #print (">>exist:" + code)
            df = pd.DataFrame.from_csv(path=DOWNLOADDIR+fileName)

            #Only FuQuan NOT happened recently, we will append data
            se = df.head(1).index #取已有文件的最近日期
            date_pre=se[0].strftime("%Y-%m-%d")
            df_pre = ts.get_h_data(str(code), start=date_pre, end=date_pre) # 前复权
            if df.head(1).open[0] == df_pre.head(1).open[0] and df.head(1).close[0] == df_pre.head(1).close[0]:
                dateNew = se[0] + datetime.timedelta(1)
                date_start = dateNew.strftime("%Y-%m-%d")
                #print date_start
                writeMode = 'a'

        if date_start == '':
            #se = get_stock_info(code)
            date_start = STOCK_LIST.timeToMarket[code0]
            date = datetime.datetime.strptime(str(date_start), "%Y%m%d")
            date_start = date.strftime('%Y-%m-%d')
        if date_start < '2012-12-31':
            date_start = '2012-12-31'
        date_end = date_end.strftime('%Y-%m-%d')  

        # 已经是最新的数据
        if date_start >= date_end:
            return None
            #df = pd.read_csv(DOWNLOADDIR+fileName)
            #return df

        print 'download ' + str(code) + ' k-line >>>begin (', date_start+u' 到 '+date_end+')'
        df_qfq = ts.get_h_data(str(code), start=date_start, end=date_end) # 前复权
        #df_nfq = ts.get_h_data(str(code), start=date_start, end=date_end, autype=None) # 不复权
        #df_hfq = ts.get_h_data(str(code), start=date_start, end=date_end, autype='hfq') # 后复权

        if df_qfq is None:
            return None

        #if df_qfq is None or df_nfq is None or df_hfq is None:
        #    return None

        #df_qfq['open_no_fq'] = df_nfq['open']
        #df_qfq['high_no_fq'] = df_nfq['high']
        #df_qfq['close_no_fq'] = df_nfq['close']
        #df_qfq['low_no_fq'] = df_nfq['low']
        #df_qfq['open_hfq']=df_hfq['open']
        #df_qfq['high_hfq']=df_hfq['high']
        #df_qfq['close_hfq']=df_hfq['close']
        #df_qfq['low_hfq']=df_hfq['low']
        
        #ma_list = [5, 20, 60]

        db.hist_dk.insert(json.loads(df_qfq.to_json(orient='records')))
        if writeMode == 'w':
            #df_qfq = df_qfq.reindex(df_qfq.index[::-1])

            #计算简单算术移动平均线MA - 注意：stock_data['close']为股票每天的收盘价
            #for ma in ma_list:
            #    df_qfq['MA' + str(ma)] = pd.rolling_mean(df_qfq['close'], ma)
            #计算指数平滑移动平均线EMA
            #for ma in ma_list:
            #    df_qfq['EMA' + str(ma)] = pd.ewma(df_qfq['close'], span=ma)
            
            #df_qfq = df_qfq.reindex(df_qfq.index[::-1])
            df_qfq.to_csv(DOWNLOADDIR+fileName)
            db.hist_dk.remove({'security':code})
            df_qfq['security']=code
            db.hist_dk.insert(json.loads(df_qfq.to_json(orient='records')))
        else:
            
            df_old = pd.DataFrame.from_csv(DOWNLOADDIR + fileName)

            # 按日期由远及近
            df_old = df_old.reindex(df_old.index[::-1])
            df_qfq = df_qfq.reindex(df_qfq.index[::-1])

            df_new = df_old.append(df_qfq)
            #print df_new

            #计算简单算术移动平均线MA - 注意：stock_data['close']为股票每天的收盘价
            #for ma in ma_list:
            #    df_new['MA' + str(ma)] = pd.rolling_mean(df_new['close'], ma)
            #计算指数平滑移动平均线EMA
            #for ma in ma_list:
            #    df_new['EMA' + str(ma)] = pd.ewma(df_new['close'], span=ma)

            # 按日期由近及远
            df_new = df_new.reindex(df_new.index[::-1])
            df_new.to_csv(DOWNLOADDIR+fileName)
            #df_qfq = df_new
            df_qfq['security']=code
            db.hist_dk.insert(json.loads(df_qfq.to_json(orient='records')))

        print '\ndownload ' + str(code) +  ' k-line finish'
        return pd.read_csv(DOWNLOADDIR+fileName)

    except Exception as e:
        print "####################",str(e)        


    return None

def download_all_stock_history_k_line():
    print 'download all stock k-line'
    try:
        STOCK_LIST = pd.DataFrame.from_csv(DOWNLOADDIR + TABLE_STOCKS_BASIC + '.csv')
        #timeToMarket = df['timeToMarket']
        pool = ThreadPool(processes=10)
        pool.map(download_stock_kline, STOCK_LIST.index)
        pool.close()
        pool.join()  

    except Exception as e:
        print str(e)
    print 'download all stock k-line'

if __name__ == '__main__':
    print 'download all stock k-line1'
    download_all_stock_history_k_line()
    #download_stock_basic_info()
