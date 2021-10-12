# -*- coding: utf-8 -*-
import json,csv,re,time,pathlib,os,sys

sys.path.append(os.getcwd())

parent_path = pathlib.Path(__file__).parent.absolute()
sys.path.append(str(parent_path))

master_path = parent_path.parent
sys.path.append(str(master_path))

project_path = master_path.parent

sys.path.append(str(project_path))

import numpy as np
from typing import List,Dict,Union,Optional
from multiprocessing import Pool, cpu_count

import requests

from tqdm import tqdm
import calendar
from datetime import datetime, timezone,date,timedelta
from dateutil.relativedelta import relativedelta

from Config.setting import Info as config

from database import query


def get_close_price(stockcode: Union[str, int],prev_close:bool=False):
    price_url = config.etnet_prev_close_price_url.format(stockcode) if prev_close else config.etnet_latest_price_url.format(stockcode)
    resp = requests.get(url=price_url)
    if resp.status_code is 200:
        close_price = resp.content.decode('utf-8').split('\n')[-1].split(',')[-1]
        try:
            close_price=float(close_price)
            return close_price
        except:
            print(f"error: stockcode : {stockcode} , close price shape :{close_price}")
            return 0
    else:
        print(f'error code: {resp.status_code} , stockcode :{str(stockcode)}')
        return 0

def dt_to_timestamp(value: Union[str,datetime]) -> int:
    naive_dt =datetime.strptime(value, "%Y-%m-%d %H:%M:%S") if isinstance(value,str) else value
    utc_dt = naive_dt.replace(tzinfo=timezone.utc)
    return int(utc_dt.timestamp())

def dt_ts_mapping(dt_lst: List[Union[str,datetime]])->Dict:
    timestamps =[dt_to_timestamp( v) for v in dt_lst]
    return dict(zip(timestamps,dt_lst))

def closet_date_func(dt_mapping:Union[Dict,List],K:Union[int,str,datetime]):
    K=K if isinstance(K,int) else dt_to_timestamp(K)
    #print(f"K :{K}")
    dt_map=dt_mapping if isinstance(dt_mapping,Dict) else dt_ts_mapping(dt_mapping)
    arr = np.array(list(dt_map.keys()))
    difference_array = np.absolute(arr - K)
    #print(f"difference_array: {difference_array}")
    index = difference_array.argmin()
    if index +1 == len(dt_mapping):
        return None
    else:

        if K>=arr[index]:
            return  dt_map[arr[index]]
        elif K>=arr[index+1]:
            return dt_map[arr[index+1]]
        elif K>=arr[index+2]:
            return dt_map[arr[index+2]]
        elif K>=arr[index+3]:
            return dt_map[arr[index+3]]
        elif K>=arr[index+4]:
            return dt_map[arr[index+4]]
        elif K>=arr[index+5]:
            return dt_map[arr[index+5]]
        elif K>=arr[index+6]:
            return dt_map[arr[index+6]]
        elif K>=arr[index+7]:
            return dt_map[arr[index+7]]
        elif K>=arr[index+8]:
            return dt_map[arr[index+8]]
        elif K>=arr[index+9]:
            return dt_map[arr[index+9]]
        elif K>=arr[index+10]:
            return dt_map[arr[index+10]]
        elif K>=arr[index+11]:
            return dt_map[arr[index+11]]
        else:
            return dt_map[arr[index+12]]
# find the index of minimum element from the array


def str2dt(date_str:str)->Union[datetime,None]:
    if len(date_str) == 10:
        return datetime.strptime((date_str + ' 00:00:00'), '%Y-%m-%d %H:%M:%S')
    elif len(date_str) == 19:
        return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
    else:
        print(f"scrapy date string must be at length of either 10 or 19, but we got {date_str}")
        return None

def trade_days_list(trade_day_url:str=config.etnet_trade_days_url):
    resp= requests.get(trade_day_url)
    decode_resp=resp.text.split('\n')[-1].split(',')[-1]
    trade_days_timestamps=[int(s)/1000 for s in decode_resp.split('|')]
    trade_days_dt=[datetime.fromtimestamp(s) for s in trade_days_timestamps]
    return trade_days_dt


def get_trackback_day(scrapy_str: Union[str, date, datetime], shareholdingdates_dt: List[datetime],     trade_day_apply: bool = False,trade_day_forward:bool=False):
    period_start = shareholdingdates_dt[-1]
    if not trade_day_apply:
        scrapy_dt = str2dt(scrapy_str) if type(scrapy_str)==str else scrapy_str

        if scrapy_dt.weekday() is not 7 and scrapy_dt >= period_start and scrapy_dt is not None:
            print(f"scrapy_dt :{scrapy_dt}, weekday:  {calendar.day_name[scrapy_dt.weekday()]} and period start : {shareholdingdates_dt[-1]}")

            lag1day = scrapy_dt
            comp_lag1day = closet_date_func(shareholdingdates_dt, lag1day)
            if comp_lag1day is not None:
                comp_lag1day = None if lag1day < period_start else shareholdingdates_dt[ shareholdingdates_dt.index(comp_lag1day) + 1]
            else:
                comp_lag1day = None

            lag5day = scrapy_dt
            comp_lag5day = closet_date_func(shareholdingdates_dt, lag5day)
            if comp_lag5day is not None:
                comp_lag5day = None if comp_lag5day - relativedelta(days=6) <= period_start else shareholdingdates_dt[
                    shareholdingdates_dt.index(comp_lag5day) + 5]
            else:
                comp_lag5day = None

        else:
            comp_lag1day,comp_lag5day=None,None

    else:
        trade_days = trade_days_list()
        trade_days = sorted(trade_days, reverse=False)
        trade_days_mid_idx = int(round(len(trade_days) / 2, 1))
        if trade_day_forward:
            trade_days_mid_idx = trade_days_mid_idx - 1
        scrapy_dt = trade_days[trade_days_mid_idx]

        comp_lag1day = trade_days[trade_days_mid_idx - 1]
        comp_lag1day = comp_lag1day if comp_lag1day > period_start else None

        comp_lag5day_idx = trade_days_mid_idx - 4 if trade_day_forward else trade_days_mid_idx - 5
        comp_lag5day = trade_days[comp_lag5day_idx]
        comp_lag5day = comp_lag5day if comp_lag5day > period_start else None


    lag1m = scrapy_dt - relativedelta(months=1)
    comp_lag1m = closet_date_func(shareholdingdates_dt, lag1m) if lag1m > period_start else None

    lag3m = scrapy_dt - relativedelta(months=3)
    comp_lag3m = closet_date_func(shareholdingdates_dt, lag3m) if lag3m > period_start else None

    lag6m = scrapy_dt - relativedelta(months=6)
    comp_lag6m = closet_date_func(shareholdingdates_dt, lag6m) if lag6m > period_start else None

    lag12m = scrapy_dt - relativedelta(months=12)
    comp_lag12m = closet_date_func(shareholdingdates_dt, lag12m) if lag12m > period_start else None

    return {"lag1day": comp_lag1day, "lag5day": comp_lag5day, "lag1m": comp_lag1m, "lag3m": comp_lag3m,"lag6m": comp_lag6m, "lag12m": comp_lag12m}


def date2dateime(dt:date):
    return datetime(year=dt.year, month=dt.month, day=dt.day, hour=0,    minute=0, second=0)

def convert2datetime(shareholdingdate:Union[date,datetime,str])->Union[datetime,None]:
    if isinstance(shareholdingdate,datetime):
        return shareholdingdate
    elif isinstance(shareholdingdate,date):
        return date2dateime(shareholdingdate)
    elif isinstance(shareholdingdate,str) and len(shareholdingdate) in [10,19]:
        return datetime.fromisoformat(shareholdingdate)
    else:
        return None

def datetime2date(dt:datetime):
    return date.fromisoformat(dt.strftime('%Y-%m-%d %H:%M:%S')[:10])

def convert2date(shareholdingdate:str)->Union[date,None]:

    if len(shareholdingdate)==10:
        return date.fromisoformat(shareholdingdate)

    elif len(shareholdingdate)==19:
        dt= datetime.fromisoformat(shareholdingdate)
        return datetime2date(dt)
    else:
        print(f' wrong format of shareholdingdate: {shareholdingdate}')
        return None

#if __name__=="__main__":

    # shareholdingdates=query.get_main_shareholdingdate()
    # shareholdingdates=[main_date for main_date in shareholdingdates if main_date.weekday() <5]
    #
    # trade_days = trade_days_list()
    # trade_days= sorted(trade_days,reverse=False)
    #
    # shareholdingdate_prev, shareholdingdate_last = [s for s in shareholdingdates if s < min(trade_days)], [s for s in shareholdingdates  if s >= min(trade_days)]
    # shareholdingdate_last = [s for s in shareholdingdate_last if s in trade_days]
    # shareholdingdates = shareholdingdate_prev + shareholdingdate_last
    # shareholdingdates = sorted(shareholdingdates, reverse=True)
    #
    # backtrack_days=get_trackback_day(scrapy_str='2021-10-05',shareholdingdates_dt=shareholdingdates,trade_day_apply=False,trade_day_forward=True)
    #
    # print(backtrack_days)