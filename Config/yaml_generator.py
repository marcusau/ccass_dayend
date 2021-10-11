# -*- coding: utf-8 -*-
import os, pathlib, sys,logging,string,csv,json,tempfile,re
import dataclasses
sys.path.append(os.getcwd())

parent_path = pathlib.Path(__file__).parent.absolute()
sys.path.append(str(parent_path))

master_path = parent_path.parent
sys.path.append(str(master_path))

project_path = master_path.parent
sys.path.append(str(project_path))

import yaml2pyclass


class Config(yaml2pyclass.CodeGenerator):
      @dataclasses.dataclass
      class HkexUrlsClass:
            @dataclasses.dataclass
            class ParticipantsClass:
                  chi: str
                  eng: str
            
            @dataclasses.dataclass
            class StocksClass:
                  chi: str
                  eng: str
            
            @dataclasses.dataclass
            class StockConnectClass:
                  hk: str
                  sh: str
                  sz: str
            
            @dataclasses.dataclass
            class MainClass:
                  ch: str
            
            participants: ParticipantsClass
            stocks: StocksClass
            stock_connect: StockConnectClass
            main: MainClass
      
      @dataclasses.dataclass
      class SchedulerClass:
            stockcodes: str
            participants: str
            stock_connect: str
            main: str
            all_func: str
      
      @dataclasses.dataclass
      class ParametersClass:
            @dataclasses.dataclass
            class MainClass:
                  max_requests: int
                  check_stockcode: int
                  back_day: int
            
            @dataclasses.dataclass
            class StockConnectClass:
                  backtract_day: int
                  to_db: bool
            
            @dataclasses.dataclass
            class StocksClass:
                  backtract_day: int
                  to_db: bool
            
            @dataclasses.dataclass
            class ParticipantsClass:
                  backtract_day: int
                  to_db: bool
            
            main: MainClass
            stock_connect: StockConnectClass
            stocks: StocksClass
            participants: ParticipantsClass
      
      @dataclasses.dataclass
      class DbClass:
            @dataclasses.dataclass
            class TablesClass:
                  stocks: str
                  participants: str
                  stock_connect: str
                  summary: str
                  main: str
            
            host: str
            port: int
            user: str
            password: str
            schema: str
            tables: TablesClass
      
      @dataclasses.dataclass
      class AbbvClass:
            @dataclasses.dataclass
            class ParticipantClass:
                  ccass_id: str
            
            participant: ParticipantClass
      
      hkex_urls: HkexUrlsClass
      etnet_latest_price_url: str
      etnet_prev_close_price_url: str
      etnet_trade_days_url: str
      log_file_path: str
      scheduler: SchedulerClass
      parameters: ParametersClass
      db: DbClass
      abbv: AbbvClass
