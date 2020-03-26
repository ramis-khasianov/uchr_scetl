import os
import json
import requests
import logging
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, DateTime, String, Float
from scetl import EdusonScetl, sql_data_types, pd_data_types

# Scetl stands for Sokols' Costyl ETL

with open('configs/configs.json') as json_file:
    configs = json.load(json_file)

# engine = create_engine('mssql+pymssql://scetl:SemperInvicta90@localhost:1433/uchr')#
db_engine = create_engine(f'sqlite:///{os.getcwd()}/db.sqlite')
logging.basicConfig(level=logging.INFO)


eduson_scetl = EdusonScetl(configs['eduson'], db_engine)
eduson_scetl.update_coursera()

