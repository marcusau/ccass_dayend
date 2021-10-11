# -*- coding: utf-8 -*-
import json, csv, re, time, pathlib, os, sys

sys.path.append(os.getcwd())

parent_path = pathlib.Path(__file__).parent.absolute()
sys.path.append(str(parent_path))

master_path = parent_path.parent
sys.path.append(str(master_path))

project_path = master_path.parent

sys.path.append(str(project_path))

import logging,click
import numpy as np
from multiprocessing import Pool, cpu_count
from typing import List, Dict, Union, Optional

import requests,calendar

from tqdm import tqdm
from datetime import datetime, timezone, date,timedelta
from dateutil.relativedelta import relativedelta

from Config.setting import Info as config
from database import query
from tools import get_trackback_day, get_close_price,trade_days_list,convert2datetime,convert2date,datetime2date,date2dateime

logger = logging.getLogger("main_dayend")
logger.setLevel(level=logging.DEBUG)

# Formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# FileHandler
file_handler = logging.FileHandler(config.log_file_path)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# StreamHandler
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


def update_main_marketcap(shareholdingdate:Union[str,datetime, date],stockcode:Union[str,int,List]=None):
    start_1=time.time()
    main_data=query.get_main_bydate(shareholdingdate=shareholdingdate)
    finish_1=time.time()
    time_1=(finish_1-start_1)/60
    logging.info(f"spend {time_1} mins for extracting {len( main_data)} data from main table")

    if stockcode:
        stockcode=[int(stockcode)] if type(stockcode) in [str,int] else [int(s) for s in stockcode]
        main_data={code: pid_data for code, pid_data in main_data.items() if int(code) in stockcode}

    start_2=time.time()
    close_prices=query.get_summary_close_bydate(shareholdingdate=shareholdingdate)
    finish_2=time.time()
    time_2=(finish_2-start_2)/60
    logging.info(f"spend {time_2} mins for extracting {len( main_data)} closing price from summary table")


    start_3=time.time()
    marketcap_sql_records=[]
    for stockcode, pid_data in tqdm(main_data.items(),desc=f"calculating market cap in main table as of {shareholdingdate}"):
        close=close_prices.get(stockcode)
        if close and close is not 0:
            marketcap_sql_record=[{'shareholdingdate': shareholdingdate, 'stockcode': stockcode, 'pid':pid,'market_cap': holding['holding'] * close} for pid,holding in main_data[stockcode].items()]
            marketcap_sql_records.extend(marketcap_sql_record)
        else:
            print(f"error : shareholdingdate: {shareholdingdate} , stockcode:{stockcode},close: {close}")
    finish_3=time.time()
    time_3=(finish_3-start_3)/60
    logging.info(f"spend {time_3} mins for calculating market cap for main table")

    start_4=time.time()
    query.update_main_mktcap(data=marketcap_sql_records)
    finish_4=time.time()
    time_4=(finish_4-start_4)/60
    logging.info(f"spend {time_4} mins for updating market cap for main table")


def update_main_change(shareholdingdate:Union[datetime, date],shareholdingdates:List[datetime],stockcode:Union[int,str,List]=None,trade_day_apply:bool=True,trade_day_forward:bool=False):
    if trade_day_apply:
        trade_days=sorted(trade_days_list(),reverse=False)
        trade_days_mid_idx = int(round(len(trade_days) / 2, 1))
        if trade_day_forward:
            trade_days_mid_idx = trade_days_mid_idx - 1
        shareholdingdate_str = trade_days[trade_days_mid_idx]
    else:
        shareholdingdate_str = shareholdingdate.strftime('%Y-%m-%d %H:%M:%S')

    past_dates = get_trackback_day(scrapy_str=shareholdingdate_str, shareholdingdates_dt=shareholdingdates,trade_day_apply=trade_day_apply,trade_day_forward=trade_day_forward)
    logging.info(f"scrapy date : {shareholdingdate}")
    logging.info(f"lag1day of scrapy date : {past_dates['lag1day']}")
    logging.info(f"lag5day of scrapy date : {past_dates['lag5day']}")
    logging.info(f"lag1m of scrapy date : {past_dates['lag1m']}")
    logging.info(f"lag3m of scrapy date : {past_dates['lag3m']}")
    logging.info(f"lag6m of scrapy date : {past_dates['lag6m']}")
    logging.info(f"lag12m of scrapy date : {past_dates['lag12m']}")

    start_retival = time.time()
    scrapy_date_pid_data = query.get_main_bydate(shareholdingdate=shareholdingdate) if past_dates[ 'lag1day'] is not None else {}
    lag1day_pid_data = query.get_main_bydate(  shareholdingdate=past_dates['lag1day'].strftime("%Y-%m-%d %H:%M:%S")[:10]) if past_dates['lag1day'] is not None else {}
    lag5day_pid_data = query.get_main_bydate(  shareholdingdate=past_dates['lag5day'].strftime("%Y-%m-%d %H:%M:%S")[:10]) if past_dates['lag5day'] is not None else {}
    lag1m_pid_data = query.get_main_bydate(shareholdingdate=past_dates['lag1m'].strftime("%Y-%m-%d %H:%M:%S")[:10]) if  past_dates['lag1m'] is not None else {}
    lag3m_pid_data = query.get_main_bydate(shareholdingdate=past_dates['lag3m'].strftime("%Y-%m-%d %H:%M:%S")[:10]) if  past_dates['lag3m'] is not None else {}
    lag6m_pid_data = query.get_main_bydate(shareholdingdate=past_dates['lag6m'].strftime("%Y-%m-%d %H:%M:%S")[:10]) if  past_dates['lag6m'] is not None else {}
    lag12m_pid_data = query.get_main_bydate(shareholdingdate=past_dates['lag12m'].strftime("%Y-%m-%d %H:%M:%S")[:10]) if past_dates['lag12m'] is not None else {}
    end_retival = time.time()
    time_retrival = (end_retival - start_retival)/60
    logging.info( f"take {time_retrival} mins on retrival of main data of scrapy date , lag1day, lag5day , lag1m, lag3m, lag6m ,lag12m")

    if stockcode:
        stockcode=[int(stockcode)] if type(stockcode) in [int,str] else [int(s) for s in stockcode]
        scrapy_date_pid_data={code:pid_holding for code, pid_holding in  scrapy_date_pid_data.items() if int(code) in stockcode}

    start_2=time.time()
    key_data = {}
    for stockcode, pid_holding in tqdm(scrapy_date_pid_data.items(), desc=f"merge dataset of different days"):

        key_data[stockcode] = {}
        lag1day_pid_holding = lag1day_pid_data.get(stockcode) if lag1day_pid_data.get(stockcode) else {}
        lag5day_pid_holding = lag5day_pid_data.get(stockcode) if lag5day_pid_data.get(stockcode) else {}
        lag1m_pid_holding = lag1m_pid_data.get(stockcode) if lag1m_pid_data.get(stockcode) else {}
        lag3m_pid_holding = lag3m_pid_data.get(stockcode) if lag3m_pid_data.get(stockcode) else {}
        lag6m_pid_holding = lag6m_pid_data.get(stockcode) if lag6m_pid_data.get(stockcode) else {}
        lag12m_pid_holding = lag12m_pid_data.get(stockcode) if lag12m_pid_data.get(stockcode) else {}

        for pid, scrapy_date_holding in pid_holding.items():
            key_data[stockcode][pid] = {'scrapy_date': scrapy_date_holding, 'lag1day': lag1day_pid_holding.get(pid),  'lag5day': lag5day_pid_holding.get(pid), 'lag1m': lag1m_pid_holding.get(pid), 'lag3m': lag3m_pid_holding.get(pid), 'lag6m': lag6m_pid_holding.get(pid), 'lag12m': lag12m_pid_holding.get(pid)}
    finish_2=time.time()
    time_2=(finish_2-start_2)/60
    logging.info( f"take {time_2} mins on calculating  lag1day, lag5day , lag1m, lag3m, lag6m ,lag12m")

    mysql_records = []
    for stockcode, pid_info in tqdm(key_data.items(), desc=f"calculate change and change percent"):

        for pid, d in pid_info.items():
            d['chg_1lag'], d['chg_5lag'], d['chg_1m'], d['chg_3m'], d['chg_6m'], d['chg_12m'] = d['scrapy_date']['holding'] - d['lag1day']['holding'] if d['scrapy_date'] and d['lag1day'] else None, d['scrapy_date']['holding'] - d['lag5day']['holding'] if d[ 'scrapy_date'] and  d['lag5day'] else None,   d['scrapy_date']['holding'] - d['lag1m']['holding'] if d['scrapy_date'] and    d['lag1m'] else None,   d['scrapy_date']['holding'] - d[ 'lag3m']['holding'] if d[ 'scrapy_date'] and  d[ 'lag3m'] else None,    d['scrapy_date']['holding'] - d['lag6m']['holding'] if d[ 'scrapy_date'] and  d['lag6m'] else None,  d['scrapy_date']['holding'] - d['lag12m']['holding'] if d['scrapy_date'] and  d['lag12m'] else None
            d['chg_pct_1lag'], d['chg_pct_5lag'], d['chg_pct_1m'], d['chg_pct_3m'], d['chg_pct_6m'], d['chg_pct_12m'] =  d['scrapy_date']['ISC_pct'] - d['lag1day']['ISC_pct'] if d['scrapy_date'] and d['lag1day']  else None, d['scrapy_date']['ISC_pct'] - d['lag5day']['ISC_pct'] if d['scrapy_date'] and d['lag5day'] else None, d['scrapy_date']['ISC_pct'] - d['lag1m']['ISC_pct']  if d['scrapy_date'] and  d['lag1m'] else None,  d['scrapy_date']['ISC_pct'] - d['lag3m']['ISC_pct']  if d['scrapy_date'] and d['lag3m'] else None, d['scrapy_date']['ISC_pct'] - d['lag6m']['ISC_pct'] if   d['scrapy_date'] and d['lag6m'] else None, d['scrapy_date']['ISC_pct'] - d['lag12m']['ISC_pct'] if d['scrapy_date'] and  d['lag12m'] else None
            d['stockcode'], d['pid'], d['shareholdingdate'] = stockcode, pid, shareholdingdate_str
            mysql_records.append(d)
    print(len(mysql_records))
    start_update_db_change = time.time()
    query.update_main_chg(data=mysql_records)
    end_update_db_change = time.time()
    time_update_db_change = (end_update_db_change - start_update_db_change)/60
    logging.info(f"take {time_update_db_change } mins on update change for main table")


def update_prev_chg_date(shareholdingdate:Union[datetime, date],shareholdingdates:List[datetime],stockcode:Union[str,int,List]=None):

    shareholdingdate_idx=shareholdingdates.index(shareholdingdate)
    if shareholdingdate_idx+1 >=len(shareholdingdates):
        logging.error(f"shareholdingdate :{shareholdingdate} , exceed the ranges of shareholdingdates :{shareholdingdates[-1]} to {shareholdingdates[0]}")
        pass
    else:
        shareholdingdate_1lag=shareholdingdates[shareholdingdate_idx+1]
        print(f'calculcating the prev_change_date for shareholdingdate :{shareholdingdate}')
        start_extract = time.time()
        records_1lag = query.select_lag1_bydate(shareholdingdate=shareholdingdate_1lag)
        records = query.select_lag1_bydate(shareholdingdate=shareholdingdate)
        finish_extract = time.time()
        time_extract = finish_extract - start_extract
        print(f"spend {time_extract} seconds to extract data from main table as of {shareholdingdate}")

        if stockcode:
            stockcode=[int(stockcode)] if type(stockcode) in [int,str] else [int(s) for s in stockcode]
            records= {code: stock_info for  code, stock_info in records.items()    if int(code) in stockcode}

        start_cal = time.time()
        for stockcode, stock_info in records.items():
            stock_info_1lag = records_1lag.get(stockcode)
            if stock_info_1lag:
                for pid, p_info in stock_info.items():
                    p_info_1lag = stock_info_1lag.get(pid)
                    if p_info_1lag:
                        p_info['prev_chg_date'] = p_info_1lag['prev_chg_date']

        mysql_records = []
        for stockcode, stock_info in records.items():
            for pid, pid_info in stock_info.items():
                pre_chg_date = shareholdingdate_1lag if pid_info['chg_1lag'] != 0 else pid_info['prev_chg_date']
                mysql_record = {'shareholdingdate': shareholdingdate, 'stockcode': stockcode, 'pid': pid,  'prev_chg_date': pre_chg_date}
                mysql_records.append(mysql_record)

        finish_cal = time.time()
        time_cal = finish_cal - start_cal
        logging.info(f"spend {time_cal} seconds to calculate prev_chg_date data from main table as of {shareholdingdate}")

        start_update_db = time.time()
        query.update_main_prev_chg_date(data=mysql_records)
        finish_update_db = time.time()
        time_update_db = (finish_update_db - start_update_db) / 60
        logging.info(f"spend {time_update_db} mins to update prev_chg_date data from main table as of {shareholdingdate}")


@click.command()
@click.option("--shareholdingdate",default=None,help='Please insert scrapy_date at format of either %Y-%m-%d %H:%M:%S or %Y-%m-%d')
@click.option("--stockcode",default=None,help='Please choose stockcode otherwise None')
@click.option("--trade_day_apply",default=True,help='Please choose stockcode otherwise None')
@click.option("--trade_day_forward",default=False,help='Please choose stockcode otherwise None')
def run_dayend(shareholdingdate:Union[str,datetime,date]=None,stockcode:Union[str,int,List]=None,trade_day_apply:bool=True,trade_day_forward:bool=False):
    shareholdingdates = query.get_main_shareholdingdate()
    shareholdingdates =[s for s in shareholdingdates if s.weekday() <5]

    latest_trade_days=trade_days_list()

    shareholdingdate_prev,shareholdingdate_last=[s for s in shareholdingdates if s<min(latest_trade_days)],[s for s in shareholdingdates if s>=min(latest_trade_days)]
    shareholdingdate_last=[s for s in shareholdingdate_last if s in latest_trade_days]
    shareholdingdates= shareholdingdate_prev+shareholdingdate_last
    shareholdingdates=sorted(shareholdingdates,reverse=True)

    if shareholdingdate is None:
        shareholdingdate = shareholdingdates[0]
    else:
        shareholdingdate=convert2datetime(shareholdingdate)

    if shareholdingdate in shareholdingdates and shareholdingdate is not None:
        print(f"shareholdingdate :{shareholdingdate}")
        # update_main_change(shareholdingdate=shareholdingdate, shareholdingdates=shareholdingdates,stockcode=stockcode,trade_day_apply=trade_day_apply,trade_day_forward=trade_day_forward)
        # update_main_marketcap(shareholdingdate=shareholdingdate,stockcode=stockcode)
        # update_prev_chg_date(shareholdingdate=shareholdingdate, shareholdingdates=shareholdingdates,stockcode=stockcode)
    else:
        logging.error(f"shareholdingdate :{shareholdingdate} is in wrong format")

if __name__=="__main__":
    run_dayend()
