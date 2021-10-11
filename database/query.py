# -*- coding: utf-8 -*-
import json,csv,re,time,pathlib,os,sys

sys.path.append(os.getcwd())

parent_path = pathlib.Path(__file__).parent.absolute()
sys.path.append(str(parent_path))

master_path = parent_path.parent
sys.path.append(str(master_path))

project_path = master_path.parent
sys.path.append(str(project_path))

from typing import Dict,List,Tuple,Union,Optional
import unicodedata
from datetime import datetime, date
from tqdm import tqdm
import mysql.connector

from Config.setting import Info as config

db_tables=config.db.tables
db_schema=config.db.schema
config = {  'host': config.db.host,
        'port': int(config.db.port),
        'database': config.db.schema,
        'user': config.db.user,
        'password': config.db.password,
        'use_unicode': True,
        'get_warnings': True,
    }

# ##----------------------------------------------------------------------------------------------------------

def get_stock_connect_date(limit:Union[int,str,None]=None):
    db = mysql.connector.connect(**config)
    cursor = db.cursor(dictionary=True)
    stmt_select = f"select distinct shareholdingdate from  {db_schema}.{db_tables.stock_connect} "

    stmt_select=stmt_select+f" order by shareholdingdate desc "
    if limit:
        stmt_select = stmt_select +f" limit {limit} "

    cursor.execute(stmt_select)
    records=[d['shareholdingdate'] for d in tqdm(cursor.fetchall(),desc=f"fetch stock_ connect shareholdingdate from db ")]
    cursor.close()
    db.close()
    return records


def get_stock_connect_bydate(shareholdingdate:Union[str,date,datetime],limit:Union[int,str,None]=None):
    db = mysql.connector.connect(**config)
    cursor = db.cursor(dictionary=True)
    stmt_select = f"select shareholdingdate, stockcode, {db_schema}.{db_tables.stock_connect}.exchange,holding, ISC_pct from  {db_schema}.{db_tables.stock_connect} where shareholdingdate='{shareholdingdate}'"

    stmt_select=stmt_select+f" order by  stockcode,{db_schema}.{db_tables.stock_connect}.exchange "
    if limit:
        stmt_select = stmt_select +f" limit {limit} "

    cursor.execute(stmt_select)
    records={f"{str(d['stockcode'])}.{str(d['exchange'])}":{'holding':d['holding'],'ISC_pct':d['ISC_pct']} for d in tqdm(cursor.fetchall(),desc=f"fetch stock_ connect from db by shareholdingdate")}
    cursor.close()
    db.close()
    return records


def update_stock_connect_chg(data:Union[List[Dict],Dict]):

    if isinstance(data,List):
        data= [(d["chg_1lag"], d["chg_5day"], d["chg_1m"], d["chg_3m"], d["chg_6m"], d["chg_12m"], d["chg_pct_1lag"],d["chg_pct_5lag"], d["chg_pct_1m"], d["chg_pct_3m"], d["chg_pct_6m"], d["chg_pct_12m"],d["shareholdingdate"],d["stockcode"],d["exchange"]) for d in data]
    else:
        data = [(data["chg_1lag"], data["chg_5day"], data["chg_1m"], data["chg_3m"], data["chg_6m"], data["chg_12m"], data["chg_pct_1lag"],data["chg_pct_5lag"], data["chg_pct_1m"], data["chg_pct_3m"], data["chg_pct_6m"], data["chg_pct_12m"],data["shareholdingdate"],data["stockcode"],data["exhchange"])]
    db = mysql.connector.connect(**config)
    cursor = db.cursor()

    stmt_insert = f"update {db_schema}.{db_tables.stock_connect}  SET chg_1lag= %s, chg_5lag= %s, chg_1m= %s, chg_3m= %s, chg_6m= %s, chg_12m = %s, chg_pct_1lag= %s, chg_pct_5lag= %s, chg_pct_1m= %s, chg_pct_3m= %s, chg_pct_6m= %s, chg_pct_12m= %s WHERE shareholdingdate= %s and stockcode= %s  and  exchange= %s "
    cursor.executemany(stmt_insert, data)
    db.commit()
    cursor.close()
    db.close()


# #####-------------------------------------------------------------------------------------------------------------------------------

def get_summary_close_bydate( shareholdingdate:[str]=None,limit:int=None):
    db = mysql.connector.connect(**config)
    cursor = db.cursor(dictionary=True )
    stmt_select = f"select stockcode, close from  {db_schema}.{db_tables.summary} "

    if shareholdingdate:
        stmt_select=stmt_select+f" where shareholdingdate = '{shareholdingdate}'"

    stmt_select = stmt_select +  " order by stockcode asc"

    if limit:
        stmt_select = stmt_select + f" limit {limit} "
    cursor.execute(stmt_select)
    records={i['stockcode']:i['close'] for i in tqdm( cursor.fetchall(),desc=f"get summary close price by limit {limit}")}
    cursor.close()
    db.close()
    return records

#
def get_summary_date( limit:Union[int,str]):
    db = mysql.connector.connect(**config)
    cursor = db.cursor(dictionary=True)
    stmt_select = f"select distinct shareholdingdate from  {db_schema}.{db_tables.summary} order by shareholdingdate desc"
    if limit:
        stmt_select =stmt_select+f" limit {limit}"
    cursor.execute(stmt_select)
    records=[d['shareholdingdate']  for d in cursor.fetchall()]
    cursor.close()
    db.close()
    return records

def update_summary_topN(data:Union[List[Dict],Dict]):

    if isinstance(data,List):
        data= [(d['top5'],d['top10'],d['shareholdingdate'],d['stockcode']) for d in data]
    else:
        data = [(data['top5'], data['top10'], data['shareholdingdate'], data['stockcode'])]
    db = mysql.connector.connect(**config)
    cursor = db.cursor()

    stmt_insert = f"update {db_schema}.{db_tables.summary}  SET top5_pct= %s,top10_pct= %s WHERE shareholdingdate= %s and stockcode= %s "
    cursor.executemany(stmt_insert, data)
    db.commit()
    cursor.close()
    db.close()


def update_summary_close(data):
    if isinstance(data, List):
        data = [(d['close'],  d['shareholdingdate'], d['stockcode']) for d in data]
    else:
        data = [(data['close'],   data['shareholdingdate'], data['stockcode'])]
    db = mysql.connector.connect(**config)
    cursor = db.cursor()

    stmt_insert = f"update {db_schema}.{db_tables.summary}  SET close= %s WHERE shareholdingdate= %s and stockcode= %s "
    cursor.executemany(stmt_insert, data)
    db.commit()
    cursor.close()
    db.close()


# ####--------------------------------------------------------------------------------------------------------------------------------------------
#
def get_main_topN_bystockcode(stockcode:Union[int,str],shareholdingdate:Union[datetime,date,str],limit:int=None,incl_participant:bool=False):
    db = mysql.connector.connect(**config)
    cursor = db.cursor(dictionary=True)
    if incl_participant:
        stmt_select =f"SELECT b.pid,a.ISC_pct FROM  {db_schema}.{db_tables.main}  a  join  {db_schema}.{db_tables.participants}  b on b.pid=a.pid where a.shareholdingdate='{shareholdingdate}' and a.stockcode= {stockcode} order by a.ISC_pct desc"
    else:
        stmt_select = f"SELECT ISC_pct  from  {db_schema}.{db_tables.main}  where  shareholdingdate = '{shareholdingdate}' and stockcode = {stockcode} order   by  ISC_pct desc"
    if limit:
        stmt_select = stmt_select + f" limit {limit} "
    cursor.execute(stmt_select)
    if incl_participant:
        records = {i['pid']: i['ISC_pct'] for i in    tqdm(cursor.fetchall(), desc=f"get main top N records by limit {limit}")}
    else:
        records =[i['ISC_pct'] for i in cursor.fetchall()]
    cursor.close()
    db.close()
    return records

def get_main_shareholdingdate(limit:int=None):
    db = mysql.connector.connect(**config)
    cursor = db.cursor(dictionary=True)
    stmt_select = f"SELECT distinct shareholdingdate FROM {db_schema}.{db_tables.main}  order by shareholdingdate desc  "
    if limit:
        stmt_select = stmt_select + f" limit {limit} "
    cursor.execute(stmt_select)
    records = [i['shareholdingdate'] for i in cursor.fetchall()]
    cursor.close()
    db.close()
    return records
#
def get_main_stockcode_bydate(shareholdingdate:Union[datetime,date,str],limit:int=None,):
    db = mysql.connector.connect(**config)
    cursor = db.cursor(dictionary=True)
    stmt_select = f"SELECT distinct stockcode FROM {db_schema}.{db_tables.main} where shareholdingdate='{shareholdingdate}' order by stockcode asc  "
    if limit:
        stmt_select = stmt_select + f" limit {limit} "
    cursor.execute(stmt_select)
    records = [i['stockcode'] for i in cursor.fetchall()]
    cursor.close()
    db.close()
    return records


def select_lag1_bydate(shareholdingdate:Union[datetime,date,str]):
    db = mysql.connector.connect(**config)
    cursor = db.cursor(dictionary=True)
    stmt_select_1 = f"SELECT stockcode, pid,chg_1lag,prev_chg_date  FROM {db_schema}.{db_tables.main}  where shareholdingdate='{shareholdingdate}' order by stockcode asc ,pid asc  "
    cursor.execute(stmt_select_1)

    records={}
    for i in cursor.fetchall():
        stockcode,pid,chg_1lag,prev_chg_date=int(i['stockcode']),int(i['pid']),i['chg_1lag'],i['prev_chg_date']
        if stockcode not in records:
            records[stockcode]={pid: {'chg_1lag':chg_1lag, 'prev_chg_date':prev_chg_date}}
        else:
            records[stockcode][pid]={'chg_1lag':chg_1lag, 'prev_chg_date':prev_chg_date}

    cursor.close()
    db.close()
    return records

def get_main_bydate(shareholdingdate:Union[datetime,date,str],limit:int=None):
    db = mysql.connector.connect(**config)
    cursor = db.cursor(dictionary=True)
    stmt_select = f"SELECT stockcode, pid,holding,ISC_pct FROM {db_schema}.{db_tables.main}  where shareholdingdate='{shareholdingdate}' order by stockcode asc   "
    if limit:
        stmt_select = stmt_select + f" limit {limit} "
    cursor.execute(stmt_select)
    records={}
    for i in cursor.fetchall():
        stockcode,pid,holding,ISC_pct =int(i['stockcode']),int(i['pid']),int(i['holding']),(float(i["ISC_pct"]) if i["ISC_pct"] else 0)
        if stockcode not in records:
            records[stockcode]={pid: {'holding':holding,'ISC_pct':ISC_pct}}
        else:
            records[stockcode][pid]={'holding':holding,'ISC_pct':ISC_pct}

    cursor.close()
    db.close()
    return records



def update_main_mktcap(data:Union[List[Dict],Dict]):
    if isinstance(data, List):
        data = [(d['market_cap'],  d['shareholdingdate'], d['stockcode'],d['pid']) for d in data]
    else:
        data = [(data['market_cap'],   data['shareholdingdate'], data['stockcode'],data['pid'])]
    db = mysql.connector.connect(**config)
    cursor = db.cursor()

    stmt_insert = f"update {db_schema}.{db_tables.main}  SET market_cap= %s WHERE shareholdingdate= %s and stockcode= %s and pid= %s "
    cursor.executemany(stmt_insert, data)
    db.commit()
    cursor.close()
    db.close()


def update_main_chg(data:Union[List[Dict],Dict]):
    if isinstance(data, List):
        data = [(d['chg_1lag'], d['chg_5lag'], d['chg_1m'], d['chg_3m'], d['chg_6m'], d['chg_12m'], d['chg_pct_1lag'], d['chg_pct_5lag'], d['chg_pct_1m'], d['chg_pct_3m'], d['chg_pct_6m'], d['chg_pct_12m'],  d['shareholdingdate'], d['stockcode'],d['pid']) for d in data]
    else:
        data = [(data['chg_1lag'], data['chg_5lag'], data['chg_1m'], data['chg_3m'], data['chg_6m'], data['chg_12m'], data['chg_pct_1lag'], data['chg_pct_5lag'], data['chg_pct_1m'], data['chg_pct_3m'], data['chg_pct_6m'], data['chg_pct_12m'],  data['shareholdingdate'], data['stockcode'],data['pid'])]
    db = mysql.connector.connect(**config)
    cursor = db.cursor()

    stmt_insert = f"update {db_schema}.{db_tables.main}  SET chg_1lag= %s,chg_5lag= %s, chg_1m= %s,chg_3m= %s, chg_6m= %s, chg_12m= %s, chg_pct_1lag= %s,  chg_pct_5lag= %s,  chg_pct_1m= %s,  chg_pct_3m= %s,  chg_pct_6m= %s,    chg_pct_12m= %s WHERE shareholdingdate= %s and stockcode= %s and pid= %s "
    cursor.executemany(stmt_insert, data)
    db.commit()
    cursor.close()
    db.close()


def update_main_prev_chg_date(data:Union[List[Dict],Dict]):
    if isinstance(data, List):
        data = [( d['prev_chg_date'],  d['shareholdingdate'], d['stockcode'],d['pid']) for d in data]

    else:
        data = [(data['prev_chg_date'],  data['shareholdingdate'], data['stockcode'],data['pid'])]
    db = mysql.connector.connect(**config)
    cursor = db.cursor()

    stmt_insert = f"update {db_schema}.{db_tables.main}  SET prev_chg_date= %s WHERE shareholdingdate= %s and stockcode= %s and pid= %s "
    cursor.executemany(stmt_insert, data)
    db.commit()
    cursor.close()
    db.close()