# -*- coding: utf-8 -*-
import json, csv, re, time, pathlib, os, sys
import click

sys.path.append(os.getcwd())

parent_path = pathlib.Path(__file__).parent.absolute()
sys.path.append(str(parent_path))

master_path = parent_path.parent
sys.path.append(str(master_path))

project_path = master_path.parent

sys.path.append(str(project_path))

from multiprocessing import Pool, cpu_count
from typing import List, Dict, Union, Optional

import numpy as np
import logging

import requests

import calendar
from tqdm import tqdm
from datetime import datetime, timezone, date,timedelta
from dateutil.relativedelta import relativedelta


from Config.setting import Info as config
from database import query as sql_query

from tools import get_trackback_day, get_close_price,trade_days_list,convert2datetime,convert2date,datetime2date,date2dateime


logger = logging.getLogger("summary_dayend")
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



def update_topn_summary(shareholdingdate:Union[str,datetime, date],stockcode:Union[int,str,List]=None):
    start=time.time()
    topN_sql_records = []
    if stockcode is None:
        main_stockcodes_bydate = sql_query.get_main_stockcode_bydate(shareholdingdate=shareholdingdate)
    else:
        main_stockcodes_bydate = [int(stockcode)] if type(stockcode) in [int,str] else [int(s) for s in stockcode]

    for main_stockcode in tqdm(main_stockcodes_bydate,desc=f"update top5 AND top10 of shareholdingdate :{shareholdingdate}"):
        main_topN = np.array(  sql_query.get_main_topN_bystockcode(stockcode=main_stockcode, shareholdingdate=shareholdingdate, limit=10))
        top5,top10=main_topN[0:5], main_topN
        top5, top10 =float(np.sum([0 if i is None else i for i in top5])),float(np.sum([0 if j is None else j for j in top10]))
        topN_sql_record = {'top5': top5, "top10": top10, "shareholdingdate": shareholdingdate, "stockcode": main_stockcode}
        topN_sql_records.append(topN_sql_record)
    finish_cal=time.time()
    cal_mins=(finish_cal-start)/60
    logging.info(f"spend {cal_mins} min for calculating top 5 and top 10 of {len(main_stockcodes_bydate)} stocks for summary table")
    start_insert_db=time.time()
    sql_query.update_summary_topN(data=topN_sql_records)
    finish_insert_db=time.time()
    insert_db_min=(finish_insert_db-start_insert_db)/60
    logging.info(f"spend {insert_db_min} min for inserting db of {len(   main_stockcodes_bydate)} stocks for summary table")




def update_close_price(shareholdingdate:Union[str,datetime,date],prev_close:bool=False,stockcode:Union[int,str,List]=None):

    if stockcode is None:
        start_extract_stockcode=time.time()
        main_stockcodes=sql_query.get_main_stockcode_bydate(shareholdingdate=shareholdingdate)
        finish_extract_stockcode=time.time()
        extract_stockcode_min=(finish_extract_stockcode-start_extract_stockcode)/60
        logging.info(f"spend {extract_stockcode_min} mins to extract stockcode from main table")
    else:
        main_stockcodes=[int(stockcode)] if type(stockcode) in [int,str] else [int(s) for s in stockcode]

    start_extract_close_price=time.time()
    mysql_data=[{'shareholdingdate':shareholdingdate__,'stockcode':stockcode,"close":get_close_price(stockcode=stockcode,prev_close=prev_close)} for shareholdingdate__, stockcode in tqdm(zip([shareholdingdate]*len(main_stockcodes),main_stockcodes),desc=f"fetch close price as of {shareholdingdate}")]
    finish_extract_close_price=time.time()
    extract_close_price_min=(finish_extract_close_price-start_extract_close_price)/60
    logging.info(f"spend {extract_close_price_min} mins to extract close price as of {shareholdingdate}")
    start_insert_db=time.time()
    sql_query.update_summary_close(data=mysql_data)
    finish_insert_db=time.time()
    insert_db_min=(finish_insert_db-start_insert_db)/60
    logging.info(f"spend {insert_db_min} mins to insert close price from summary table")


@click.command()
@click.option("--shareholdingdate",default=None,help='Please insert scrapy_date at format of either %Y-%m-%d %H:%M:%S or %Y-%m-%d')
@click.option("--prev_close",default=False,help='Please choose True or False for prev_close')
@click.option("--stockcode",default=None,help='Please choose stockcode or stockcodes otherwise None')
def run_dayend(shareholdingdate:Union[str,datetime,date]=None,prev_close:bool=False,stockcode:Union[int,str,List]=None):
    shareholdingdates = sql_query.get_summary_date(2000)
    shareholdingdates = [s for s in shareholdingdates if s.weekday() < 5]

    latest_trade_days=trade_days_list()
    shareholdingdate_prev,shareholdingdate_last=[s for s in shareholdingdates if s<min(latest_trade_days)],[s for s in shareholdingdates if s>=min(latest_trade_days)]
    shareholdingdate_last=[s for s in shareholdingdate_last if s in latest_trade_days]
    shareholdingdates= shareholdingdate_prev+shareholdingdate_last
    shareholdingdates=sorted(shareholdingdates,reverse=True)

    if shareholdingdate is None:
        shareholdingdate = shareholdingdates[0]
    else:
        shareholdingdate = convert2datetime(shareholdingdate)

    if shareholdingdate in shareholdingdates:
        print(f"shareholdingdate : {shareholdingdate}")
        # update_topn_summary(shareholdingdate=shareholdingdate,stockcode=stockcode)
        # logging.info( f" finished update top5 columns and top 10 columns in summary table as of shareholdingdate : {shareholdingdate}")
        # update_close_price(shareholdingdate=shareholdingdate,prev_close=prev_close,stockcode=stockcode)
        # logging.info(f" finished update close price columns in summary table as of shareholdingdate : {shareholdingdate}")
    else:
        logging.error(f"shareholdingdate :{shareholdingdate} is in wrong format")


if __name__=='__main__':
   run_dayend()
    # import csv
    # close=[]
    # csv_path=r'C:\Users\marcus\PycharmProjects\ccass_dev\HKEx-NoAdjDaily.csv'
    # with open(csv_path,'r',encoding='utf-8') as f:
    #     data=csv.reader(f, delimiter = ",")
    #     i=0
    #     for d in data:
    #         if i==0:
    #             header=d
    #         else:
    #             d_=dict(zip(header,d))
    #             close.append(d_)
    #         i=+1
    #
    # target_dt=datetime(year=2021,month=1,day=1,hour=0,minute=0,second=0)
    # bound_dt=target_dt+timedelta(days=100)
    # close=[c for c in close if bound_dt>datetime.fromisoformat(c['CDATE'])>=target_dt ]
    # records=[]
    #
    # for c in close [: 20]:
    #     record = {'shareholdingdate': '', 'stockcode': '', 'close': ''}
    #     record['stockcode'],record['shareholdingdate'],record['close']=int(c.get('CSTOCKCD',0)),target_dt,float(c.get('CCLOSE',0.0))
    #     if record['close'] != 0.0:
    #         records.append(record)
    # sql_query.update_summary_close(records)
    #
