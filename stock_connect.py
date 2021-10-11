# -*- coding: utf-8 -*-
import json, csv, re, time, pathlib, os, sys
import calendar
import numpy as np

sys.path.append(os.getcwd())

parent_path = pathlib.Path(__file__).parent.absolute()
sys.path.append(str(parent_path))

master_path = parent_path.parent
sys.path.append(str(master_path))

project_path = master_path.parent

sys.path.append(str(project_path))
import requests,logging,click


from multiprocessing import Pool, cpu_count
from typing import List, Dict, Union, Optional

from tqdm import tqdm
from datetime import datetime, timezone, date,timedelta
from dateutil.relativedelta import relativedelta

from Config.setting import Info as config
from database import query as sql_query

from tools import get_trackback_day,str2dt,trade_days_list,convert2datetime,date2dateime,datetime2date


logger = logging.getLogger("stock_connect_dayend")
logger.setLevel(logging.DEBUG)

# Formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# FileHandler
file_handler = logging.FileHandler(config.log_file_path)
print(f"log filepath :{config.log_file_path}")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# StreamHandler
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)




def update_stock_connect_change(shareholdingdate:Union[str,date,datetime],shareholdingdate_dt:List,stockcode:Union[int,str,List]=None,trade_day_apply:bool=True,trade_day_forward:bool=False):

    if trade_day_apply:
        trade_days = trade_days_list()
        trade_days = sorted(trade_days, reverse=False)
        trade_days_mid_idx = int(round(len(trade_days) / 2, 1))
        if trade_day_forward:
            trade_days_mid_idx = trade_days_mid_idx - 1
        shareholdingdate = trade_days[trade_days_mid_idx]
    else:
        shareholdingdate = shareholdingdate
    past_dates = get_trackback_day(scrapy_str=shareholdingdate, shareholdingdates_dt=shareholdingdate_dt,trade_day_apply=trade_day_apply,trade_day_forward=trade_day_forward)

    start_2=time.time()
    shareholdingdate_data= sql_query.get_stock_connect_bydate(shareholdingdate=shareholdingdate)# if i<=datetime(year=2020,month=5,day=5) ]
    lag1day_data=sql_query.get_stock_connect_bydate(shareholdingdate=past_dates['lag1day']) if past_dates['lag1day'] is not None else {}
    lag5day_data=sql_query.get_stock_connect_bydate(shareholdingdate=past_dates['lag5day']) if past_dates['lag5day'] is not None else {}
    lag1m_data=sql_query.get_stock_connect_bydate(shareholdingdate=past_dates['lag1m']) if past_dates['lag1m'] is not None else {}
    lag3m_data=sql_query.get_stock_connect_bydate(shareholdingdate=past_dates['lag3m']) if past_dates['lag3m'] is not None else {}
    lag6m_data=sql_query.get_stock_connect_bydate(shareholdingdate=past_dates['lag6m']) if past_dates['lag6m'] is not None else {}
    lag12m_data=sql_query.get_stock_connect_bydate(shareholdingdate=past_dates['lag12m']) if past_dates['lag12m'] is not None else {}
    print(f"1day: {past_dates['lag1day']},   5day:{past_dates['lag5day']} ,  1m:{past_dates['lag1m']} , 3m: {past_dates['lag3m']} , 6m: {past_dates['lag6m']} , 12m: {past_dates['lag12m']}")
    finish_2=time.time()
    time_2=finish_2-start_2
    logging.info(f"spend {time_2} mins on extracting scrapy date from stock connect table")
    print(f"spend {time_2} mins on extracting scrapy date from stock connect table")

    shareholdingdate_holdings= {stockcode:d.get('holding') for stockcode, d in shareholdingdate_data.items()}
    lag1day_hs={stockcode:d.get('holding') for stockcode, d in lag1day_data.items()}
    lag5day_hs={stockcode:d.get('holding') for stockcode, d in lag5day_data.items()}
    lag1m_hs={stockcode:d.get('holding') for stockcode, d in lag1m_data.items()}
    lag3m_hs={stockcode:d.get('holding') for stockcode, d in lag3m_data.items()}
    lag6m_hs={stockcode:d.get('holding') for stockcode, d in lag6m_data.items()}
    lag12m_hs={stockcode:d.get('holding') for stockcode, d in lag12m_data.items()}

    shareholdingdate_ISC_pcts= {stockcode:d.get('ISC_pct') for stockcode, d in shareholdingdate_data.items()}
    lag1day_ISC_pcts={stockcode:d.get('ISC_pct') for stockcode, d in lag1day_data.items()}
    lag5day_ISC_pcts={stockcode:d.get('ISC_pct') for stockcode, d in lag5day_data.items()}
    lag1m_ISC_pcts={stockcode:d.get('ISC_pct') for stockcode, d in lag1m_data.items()}
    lag3m_ISC_pcts={stockcode:d.get('ISC_pct') for stockcode, d in lag3m_data.items()}
    lag6m_ISC_pcts={stockcode:d.get('ISC_pct') for stockcode, d in lag6m_data.items()}
    lag12m_ISC_pcts={stockcode:d.get('ISC_pct') for stockcode, d in lag12m_data.items()}



    if stockcode:
        stockcode=[int(stockcode)] if type(stockcode) in [int,str] else [int(s) for s in stockcode]
        shareholdingdate_data={code: shareholdingdate_d for  code, shareholdingdate_d in  shareholdingdate_data.items() if int(code) in stockcode}

    start_4=time.time()
    stock_connect_sql_records=[]
    for stock_code,shareholdingdate_d in tqdm(shareholdingdate_data.items(),desc='calculating change and % change of stock connect table'):


        sd_h, sd_isc_pct= shareholdingdate_d.get('holding'),shareholdingdate_d.get('ISC_pct')
        lag1day_h,lag1day_isc_pct= lag1day_hs.get(stock_code), lag1day_ISC_pcts.get(stock_code)
        lag5day_h,lag5day_isc_pct= lag5day_hs.get(stock_code), lag5day_ISC_pcts.get(stock_code)
        lag1m_h,lag1m_isc_pct= lag1m_hs.get(stock_code), lag1m_ISC_pcts.get(stock_code)
        lag3m_h,lag3m_isc_pct= lag3m_hs.get(stock_code), lag3m_ISC_pcts.get(stock_code)
        lag6m_h,lag6m_isc_pct= lag6m_hs.get(stock_code), lag6m_ISC_pcts.get(stock_code)
        lag12m_h,lag12m_isc_pct= lag12m_hs.get(stock_code), lag12m_ISC_pcts.get(stock_code)

        chg_1lag, chg_5lag, chg_1m, chg_3m, chg_6m, chg_12m =  sd_h-lag1day_h if sd_h and lag1day_h else None, sd_h-lag5day_h if sd_h and lag5day_h else None, sd_h-lag1m_h if sd_h and lag1m_h else None, sd_h-lag3m_h if sd_h and lag3m_h else None, sd_h-lag6m_h if sd_h and lag6m_h else None, sd_h-lag12m_h if sd_h and lag12m_h else None,
        chg_pct_1lag, chg_pct_5lag, chg_pct_1m, chg_pct_3m, chg_pct_6m, chg_pct_12m = sd_isc_pct-lag1day_isc_pct if sd_isc_pct and lag1day_isc_pct else None, sd_isc_pct-lag5day_isc_pct if sd_isc_pct and lag5day_isc_pct else None, sd_isc_pct-lag1m_isc_pct if sd_isc_pct and lag1m_isc_pct else None, sd_isc_pct-lag3m_isc_pct if sd_isc_pct and lag3m_isc_pct else None, sd_isc_pct-lag6m_isc_pct if sd_isc_pct and lag6m_isc_pct else None, sd_isc_pct-lag12m_isc_pct if sd_isc_pct and lag12m_isc_pct else None

        code, ex_ = stock_code.split('.')
        if 'HK' in ex_:
            ex_ = 'HK'
        elif 'SH' in ex_:
            ex_ = 'SH'
        else:
            ex_= 'SZ'
        stock_code=code+'.'+ex_
        
        stock_connect_sql_record={"shareholdingdate":shareholdingdate,"stockcode":stock_code.split('.')[0],'exchange':stock_code.split('.')[-1],  "chg_1lag":chg_1lag, "chg_5day":chg_5lag, "chg_1m":chg_1m, "chg_3m":chg_3m, "chg_6m":chg_6m, "chg_12m":chg_12m, "chg_pct_1lag":chg_pct_1lag, "chg_pct_5lag":chg_pct_5lag, "chg_pct_1m":chg_pct_1m, "chg_pct_3m":chg_pct_3m, "chg_pct_6m":chg_pct_6m, "chg_pct_12m":chg_pct_12m}
        #print(stock_connect_sql_record)
        stock_connect_sql_records.append(stock_connect_sql_record)
    finish_4=time.time()
    time_4=finish_4-start_4
    logging.info(f"spend {time_4} mins on calculating change")
    print(f"spend {time_4} mins on calculating change\n")
    #
    # for i in stock_connect_sql_records[:5]:
    #     print(i)
    start_5=time.time()
    sql_query.update_stock_connect_chg(stock_connect_sql_records)
    finish_5=time.time()
    time_5=finish_5-start_5
    logging.info(f"spend {time_5} mins on updating change data in stock_connect table")
    print(f"spend {time_5} mins on updating change data in stock_connect table")

@click.command()
@click.option("--shareholdingdate",default=None,help='Please insert scrapy_date at format of either %Y-%m-%d %H:%M:%S or %Y-%m-%d')
@click.option("--stockcode",default=None,help='Please choose stockcode otherwise None')
@click.option("--trade_day_apply",default=True,help='Please choose stockcode otherwise None')
@click.option("--trade_day_forward",default=False,help='Please choose stockcode otherwise None')
def run_dayend(shareholdingdate:Union[str,datetime,date]=None,stockcode:Union[int,str,List]=None,trade_day_apply:bool=True,trade_day_forward:bool=False):


    shareholdingdates = sql_query.get_stock_connect_date()
    shareholdingdates = [s for s in shareholdingdates if s.weekday() < 5]

    latest_trade_days=trade_days_list()
    shareholdingdate_prev,shareholdingdate_last=[s for s in shareholdingdates if s<min(latest_trade_days)],[s for s in shareholdingdates if s>=min(latest_trade_days)]
    shareholdingdate_last=[s for s in shareholdingdate_last if s in latest_trade_days]
    shareholdingdates= shareholdingdate_prev+shareholdingdate_last
    shareholdingdates=sorted(shareholdingdates,reverse=True)

    if shareholdingdate is None:
        shareholdingdate=shareholdingdates[0]

    else:
        shareholdingdate=convert2datetime(shareholdingdate)

    if shareholdingdate in shareholdingdates and shareholdingdate is not None:
        logging.info(f"shareholdingdate : {shareholdingdate}")
        print(f"shareholdingdate : {shareholdingdate}")
        start_1 = time.time()
        update_stock_connect_change(shareholdingdate, shareholdingdates,stockcode=stockcode,trade_day_apply=trade_day_apply,trade_day_forward=trade_day_forward)
        finish_1=time.time()
        time_1 = finish_1 - start_1
        logging.info(f"spend {time_1} seconds on updating change and % change from stock_connect table")
        print(f"spend {time_1} seconds on updating change and % change from stock_connect table")
    else:
        logging.error(f"shareholdingdate :{shareholdingdate} is not in database, please change another shareholdingdate")
        print(f"shareholdingdate :{shareholdingdate} is not in database, please change another shareholdingdate")



if __name__=="__main__":

           run_dayend()
