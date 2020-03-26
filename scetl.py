import os
import json
import requests
import logging
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, DateTime, String, Float

# Scetl stands for Sokols' Costyl ETL

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

# ------------
# --Coursera--
# ------------


with open('configs/configs.json') as json_file:
    configs = json.load(json_file)

# engine = create_engine('mssql+pymssql://scetl:SemperInvicta90@localhost:1433/uchr')#
db_engine = create_engine(f'sqlite:///{os.getcwd()}/db.sqlite')
logging.basicConfig(level=logging.INFO)

config = configs['coursera']


class CourseraScetl:
    access_token = None
    access_token_updated_at = None

    def __init__(self, config, engine):
        self.urls = config['urls']
        self.config = config
        self.engine = engine

    def check_token_freshness(self):
        if os.path.exists('configs/coursera_token.json'):
            with open('configs/coursera_token.json') as token_json:
                access_token_dict = json.load(token_json)
            logging.info(f'Token dict: {access_token_dict}')
            token_last_update_time = datetime.strptime(access_token_dict['date_updated'], '%Y-%m-%d %H:%M')
            token_lifetime = (datetime.utcnow() - token_last_update_time).total_seconds()
            logging.info(f'Access token is {token_lifetime} seconds old')
            if token_lifetime < 1200:  # token has lifetime of 1800 sec and update has 900-1200 sec cool down
                self.access_token = access_token_dict['access_token']
                self.access_token_updated_at = access_token_dict['date_updated']
                logging.info('Access is fresh')
                return True
        logging.info('Access token needs to be updated')
        return False

    def get_access_token(self):
        url = self.urls['get_access_token']['url']
        body = self.urls['get_access_token']['body_params']
        response = requests.post(url, data=body).json()

        new_token_dict = {
            'access_token': response['access_token'],
            'date_updated': datetime.strftime(datetime.utcnow(), '%Y-%m-%d %H:%M')
        }
        logging.info(f'got response {new_token_dict}')

        with open('configs/coursera_token.json', 'w') as file:
            file.write(json.dumps(new_token_dict))

        self.access_token = new_token_dict['access_token']
        self.access_token_updated_at = datetime.utcnow()


coursera_scetl = CourseraScetl(configs['coursera'], db_engine)
coursera_scetl.check_token_freshness()
coursera_scetl.get_access_token()


