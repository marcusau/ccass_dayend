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
import numpy as np
from datetime import datetime, date
from tqdm import tqdm
import mysql.connector
from Config.setting import Info as config
config = {
        'host': config.db.host,
        'port': int(config.db.port),
        'database': config.db.schema,
        'user': config.db.user,
        'password': config.db.password,
        'use_unicode': True,
        'get_warnings': True,
    }

db_tables=config.db.tables
db_schema=config.db.schema

db = mysql.connector.connect(**config)
cursor = db.cursor(dictionary=True)
select_date='2020-09-30'
stmt_select = f"select shareholdingdate,stockcode,pid,shareholding  from  {'ccass'}.{'main'} where shareholdingdate >='{select_date}' and shareholdingdate <='{select_date}'  limit 470000"
cursor.execute(stmt_select)

records=[d for d in tqdm(cursor.fetchall(),desc=f"fetch stock data from db")]

stmt_insert = f"select shareholdingdate,stockcode,pid,shareholding  from  {'ccass_dev'}.{'main'} "
cursor.executemany(stmt_select,records)
cursor.close()
db.close()