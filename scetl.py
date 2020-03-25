import os
import json
import requests
import logging
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, DateTime, String, Float

# Scetl stands for Sokols' Costyl ETL

with open('configs/configs.json') as json_file:
    configs = json.load(json_file)

# engine = create_engine('mssql+pymssql://scetl:SemperInvicta90@localhost:1433/uchr')#
db_engine = create_engine(f'sqlite:///{os.getcwd()}/db.sqlite')
logging.basicConfig(level=logging.INFO)
'''
df = pd.DataFrame([{'id': 1, 'name': 'ramis'}, {'id': 2, 'name': 'babis'}])
df.to_sql('test', con=engine, if_exists='replace', index=False)
config = configs['eduson']
'''

sql_data_types = {'INT': Integer, 'VARCHAR': String, 'NUMERIC': Float, 'DATETIME': DateTime, }
pd_data_types = {'INT': 'int', 'VARCHAR': 'object', 'NUMERIC': 'float', 'DATETIME': 'datetime'}

# ------------
# ---Eduson---
# ------------


class EdusonScetl:

    def __init__(self, config, engine):
        self.urls = config['urls']
        self.config = config
        self.engine = engine

    def apply_data_types(self, table, df):
        cols = self.config['tables'][table]['columns']
        col_dict = {x['name']: pd_data_types[x['type']] for x in cols}
        for col in df.columns:
            if col_dict[col] == 'datetime':
                df[col] = pd.to_datetime(df[col], yearfirst=True)
                df[col] = df[col].dt.tz_localize(None)
            else:
                df[col] = df[col].astype(col_dict[col])
        return df

    def check_tables(self):
        metadata = MetaData(self.engine)
        tables = self.config['tables']
        for db_table in tables:
            if not self.engine.dialect.has_table(self.engine, tables[db_table]['table_name']):
                logging.info(f'No table {db_table}, creating one')
                columns = [Column(col['name'], sql_data_types[col['type']]) for col in tables[db_table]['columns']]
                Table(tables[db_table]['table_name'], metadata, *columns)
        metadata.create_all()

    def get_user_json(self):
        headers = {
            self.config['request_headers']['header_name']: self.config['request_headers']['header_value']
        }
        url = self.urls['users']['url']
        logging.info(f'Calling {url}')
        response = requests.get(url, headers=headers).json()
        return response

    def get_user_courses_json(self, user_id):
        headers = {
            self.config['request_headers']['header_name']: self.config['request_headers']['header_value']
        }
        url = self.urls['user_courses']['url'].replace('{id}', str(user_id))
        response = requests.get(url, headers=headers).json()
        return response

    def update_users(self):
        table_dict = self.config['tables']['users']
        table_cols = [x['name'] for x in table_dict['columns']]
        table_name = table_dict['table_name']

        df_initial_users = pd.DataFrame(self.get_user_json())
        df_initial_users['last_update'] = datetime.utcnow()
        df_initial_users = df_initial_users[table_cols]
        df_initial_users = self.apply_data_types('users', df_initial_users)
        df_initial_users.to_sql(table_name, con=self.engine, if_exists='replace', index=False)

    def update_user_courses(self, user_id):
        table_dict = self.config['tables']['user_courses']
        table_cols = [x['name'] for x in table_dict['columns']]
        table_name = table_dict['table_name']

        response = self.get_user_courses_json(user_id)
        if response['courses']:
            df_user_courses = pd.DataFrame(response['courses'])
            df_user_courses['last_update'] = datetime.utcnow()
            df_user_courses['user_id'] = user_id
            df_user_courses = df_user_courses[table_cols]
            df_user_courses = self.apply_data_types('user_courses', df_user_courses)

            with self.engine.connect() as connection:
                query = f'DELETE FROM eduson_user_courses WHERE user_id = {user_id}'
                connection.execute(query)
            df_user_courses.to_sql(table_name, con=self.engine, if_exists='append', index=False)

    def update_user_changes(self):
        table_dict = self.config['tables']['user_changes']
        table_cols = [x['name'] for x in table_dict['columns']]
        table_name = table_dict['table_name']

        self.check_tables()
        with self.engine.connect() as connection:
            query = 'SELECT MAX(updated_at) FROM eduson_user_changes'
            last_update_ts = connection.execute(query).fetchone()[0]

        df_new_user_changes = pd.DataFrame(self.get_user_json())
        df_new_user_changes['last_update'] = datetime.utcnow()
        df_new_user_changes = df_new_user_changes[table_cols]
        df_new_user_changes = self.apply_data_types('user_changes', df_new_user_changes)

        if last_update_ts is None:
            df_new_user_changes.to_sql(table_name, con=self.engine, index=False, if_exists='replace')
        else:
            logging.info(f'updating data from {last_update_ts}')
            last_update_ts = pd.to_datetime(last_update_ts, yearfirst=True)
            df_new_user_changes = df_new_user_changes[df_new_user_changes['updated_at'] > last_update_ts].copy()
            df_new_user_changes.to_sql(table_name, con=self.engine, index=False, if_exists='append')

        for user_id in df_new_user_changes['id']:
            logging.info(f'Getting data for user {user_id}')
            self.update_user_courses(user_id)

    def update_coursera(self):
        self.update_users()
        self.update_user_changes()


eduson_scetl = EdusonScetl(configs['eduson'], db_engine)
eduson_scetl.update_coursera()
