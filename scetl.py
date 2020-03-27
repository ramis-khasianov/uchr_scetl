import os
import json
import requests
import logging
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, DateTime, String, Float

# -----------------------------------
# Scetl stands for Sokols' Costyl ETL
# -----------------------------------


class Scetl:

    # Data type to create tables if none exist in database
    sql_data_types = {
        'INT': Integer,
        'VARCHAR': String,
        'NUMERIC': Float,
        'DATETIME': DateTime,
        'UNIXTIME': DateTime
    }

    # Data type to convert pandas data frames
    pd_data_types = {
        'INT': 'int',
        'VARCHAR': 'object',
        'NUMERIC': 'float',
        'DATETIME': 'datetime',
        'UNIXTIME': 'unixtime'
    }

    def __init__(self, config, engine):
        """
        Class needs two things - configs as dict and sqlalchemy engine
        :param config: configs as python dict parsed from json
        :param engine: sqlalchemy engine made from create_engine
        """
        self.urls = config['urls']
        self.config = config
        self.engine = engine

    def apply_data_types(self, table, df):
        """
        Transforms pd.DataFrame data types in accordance with schema provided in config-tables
        :param table: table dict from config dict (not table_name)
        :param df: DataFrame to process, usually df made from api call json
        :return: same df with proper data types
        """
        cols = self.config['tables'][table]['columns']
        col_dict = {x['name']: self.pd_data_types[x['type']] for x in cols}
        for col in df.columns:
            if col_dict[col] == 'datetime':
                df[col] = pd.to_datetime(df[col], yearfirst=True)
                df[col] = df[col].dt.tz_localize(None)
            elif col_dict[col] == 'unixtime':
                df[col] = pd.to_datetime(df[col], unit='ms')
            else:
                df[col] = df[col].astype(col_dict[col])
        return df

    def check_tables(self):
        """
        Check if tables provided in configs exist and if not - create them with proper data types (sql_data_types)
        :return: None, writes results to db
        """
        metadata = MetaData(self.engine)
        tables = self.config['tables']
        for db_table in tables:
            if not self.engine.dialect.has_table(self.engine, tables[db_table]['table_name']):
                logging.info(f'No table {db_table}, creating one')
                columns = [Column(col['name'], self.sql_data_types[col['type']]) for col in tables[db_table]['columns']]
                Table(tables[db_table]['table_name'], metadata, *columns)
        metadata.create_all()

    def get_table_params(self, table):
        """
        Unpacks table name and column names from config dict
        :param table: table dict from config dict (not table_name)
        :return: table name as string and table columns as list
        """
        table_dict = self.config['tables'][table]
        table_cols = [x['name'] for x in table_dict['columns']]
        table_name = table_dict['table_name']
        return table_name, table_cols

    def get_last_update_ts(self, table_name):
        """
        Several calls require to get date of last update from database to check if data should be added
        This function returns last updated date from database
        :param table_name: table name from database
        :return: max datetime of last_update column for this table
        """
        with self.engine.connect() as connection:
            query = f'SELECT MAX(last_update) FROM {table_name}'
            last_update_ts = connection.execute(query).fetchone()[0]
            last_update_ts = pd.to_datetime(last_update_ts, yearfirst=True)
        return last_update_ts


# ------------
# ---Eduson---
# ------------


class EdusonScetl(Scetl):

    def get_user_json(self):
        """
        Make an api call to eduson/users endpoint and return python dict from response
        :return: dict with data from api call
        """
        headers = {
            self.config['request_headers']['header_name']: self.config['request_headers']['header_value']
        }
        url = self.urls['users']['url']
        response = requests.get(url, headers=headers).json()
        return response

    def get_user_courses_json(self, user_id):
        """
        Make an api call to eduson/user/courses endpoint and return python dict from response
        :param user_id: path variable for api call and unique id for user in eduson
        :return: dict with data from api call
        """
        headers = {
            self.config['request_headers']['header_name']: self.config['request_headers']['header_value']
        }
        url = self.urls['user_courses']['url'].replace('{id}', str(user_id))
        response = requests.get(url, headers=headers).json()
        return response

    def update_users(self, user_json):
        """
        Clean and save results of get_user_json to database
        :param user_json: result from get_user_json call
        :return: None, writes results to db
        """
        table_name, table_cols = self.get_table_params('users')

        df_initial_users = pd.DataFrame(user_json)
        df_initial_users['last_update'] = datetime.utcnow()
        df_initial_users = df_initial_users[table_cols]
        df_initial_users = self.apply_data_types('users', df_initial_users)
        df_initial_users.to_sql(table_name, con=self.engine, if_exists='replace', index=False)

    def update_user_courses(self, user_id):
        """
        Updating user courses data if user had any activity since last update
        Function calls for json, removes all previous data for this user courses and writes json data to db
        :param user_id: user id for Eduson
        :return: None, writes results to db
        """
        table_name, table_cols = self.get_table_params('user_courses')

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
        """
        One of the main functions.
        Calls other functions - creating tables, updating user table, updating courses for users that have changes
        :return: None, writes results to db
        """
        table_name, table_cols = self.get_table_params('user_changes')

        self.check_tables()
        last_update_ts = self.get_last_update_ts(table_name)

        # utilizing single json for updating user table and populating user changes table
        user_json = self.get_user_json()
        self.update_users(user_json)

        df_new_user_changes = pd.DataFrame(user_json)
        df_new_user_changes['last_update'] = datetime.utcnow()
        df_new_user_changes = df_new_user_changes[table_cols]
        df_new_user_changes = self.apply_data_types('user_changes', df_new_user_changes)

        if last_update_ts is None:
            df_new_user_changes.to_sql(table_name, con=self.engine, index=False, if_exists='replace')
        else:
            logging.info(f'updating data from {last_update_ts}')
            df_new_user_changes = df_new_user_changes[df_new_user_changes['updated_at'] > last_update_ts].copy()
            df_new_user_changes.to_sql(table_name, con=self.engine, index=False, if_exists='append')

        for user_id in df_new_user_changes['id']:
            logging.info(f'Getting data for user {user_id}')
            self.update_user_courses(user_id)

    def update_scetl(self):
        """
        Start data update routines
        :return: None, calls functions that write results to db
        """
        self.update_user_changes()

# ------------
# --Coursera--
# ------------


class CourseraScetl(Scetl):

    # Unlike other servises in UC HR coursera's API token needs to be refreshed using 'refresh_token' on 30 min basis
    access_token = None
    access_token_updated_at = None

    def check_token_freshness(self):
        """
        Checks if access token needs refreshing
        Current access token is stored in configs/coursera_token.json
        :return: Bool, True is token is fresh and valid
        """
        if os.path.exists('configs/coursera_token.json'):
            with open('configs/coursera_token.json') as token_json:
                access_token_dict = json.load(token_json)
            token_last_update_time = datetime.strptime(access_token_dict['date_updated'], '%Y-%m-%d %H:%M')
            token_lifetime = (datetime.utcnow() - token_last_update_time).total_seconds()
            logging.info(f'Access token is {token_lifetime} seconds old')
            if token_lifetime < 1200:  # token has lifetime of 1800 sec and update has 900-1200 sec cool down
                self.access_token = access_token_dict['access_token']
                self.access_token_updated_at = access_token_dict['date_updated']
                logging.info('Access is fresh, no need for update')
                return True
        logging.info('Access token needs to be updated')
        return False

    def get_access_token(self):
        """
        Refreshes access_token using refresh_token
        :return: None, updates class's attributes
        """
        url = self.urls['get_access_token']['url']
        body = self.urls['get_access_token']['body_params']
        response = requests.post(url, data=body).json()

        new_token_dict = {
            'access_token': response['access_token'],
            'date_updated': datetime.strftime(datetime.utcnow(), '%Y-%m-%d %H:%M')
        }

        with open('configs/coursera_token.json', 'w') as file:
            file.write(json.dumps(new_token_dict))

        self.access_token = new_token_dict['access_token']
        self.access_token_updated_at = datetime.utcnow()

    def get_coursera_request_headers(self):
        """
        Updates coursera's api endpoints since they require org_id in it's path
        :return:
        """
        header_name = self.config['request_headers']['header_name']
        header_value = self.config['request_headers']['header_value']
        if not self.check_token_freshness():
            self.get_access_token()
        header_value = header_value.replace('{access_token}', self.access_token)
        return {header_name: header_value}

    def get_paged_json(self, url):
        """
        Makes paginated response from any of coursera api calls and returns one long list of contents
        :param url: url of api call
        :return: list of response's 'elements'
        """
        headers = self.get_coursera_request_headers()
        org_id = self.config['global_params']['path_variables']['orgId']
        start = int(self.config['global_params']['params']['start'])
        limit = int(self.config['global_params']['params']['limit'])
        url = url.replace('{orgId}', org_id)
        total_records = 0
        page = 0
        paged_response = {}

        while start <= total_records:
            params = {
                'start': start,
                'limit': limit
            }
            page += 1
            logging.info(f'updating page {page}: calling {url} with params {params}')
            response = requests.get(url, headers=headers, params=params).json()
            start += limit
            total_records = int(response['paging']['total'])
            paged_response[f'page_{page}'] = response

        concat_response = [item for x in paged_response.keys() for item in paged_response[x]['elements']]
        return concat_response

    def get_enrolments_json(self):
        """
        Calls get_paged_json function for enrolments url
        :return: list, result of get_paged_json
        """
        return self.get_paged_json(self.urls['enrolments']['url'])

    def get_contents_json(self):
        """
        Calls get_paged_json function for contents url
        :return: list, result of get_paged_json
        """
        return self.get_paged_json(self.urls['contents']['url'])

    def update_enrolments(self, enrolments_json=None):
        """
        Clean and save results of get_enrolments_json to database
        :param enrolments_json:
        :return: None, writes results to db
        """
        table_name, table_cols = self.get_table_params('enrolments')
        logging.info(f'Updating {table_name}')
        if enrolments_json is None:
            enrolments_json = self.get_enrolments_json()

        df = pd.DataFrame(enrolments_json)
        df['last_update'] = datetime.utcnow()
        df = self.apply_data_types('enrolments', df[table_cols])
        df.to_sql(table_name, con=self.engine, if_exists='replace', index=False)

    def update_contents(self):
        """
        Clean and save results of get_contents_json to database
        Note that it takes a while since all coursera courses available to Uralchem is processed
        :return: None, writes results to db
        """
        contents_json = self.get_contents_json()
        df = pd.DataFrame(contents_json)

        def extract_metadata(metadata_dict):
            if metadata_dict['typeName'] == 'courseMetadata':
                return metadata_dict['definition']['estimatedLearningTime']
            else:
                return [x['contentId'] for x in metadata_dict['definition']['courseIds']]

        # getting relationship between specializations and courses
        table_name, table_cols = self.get_table_params('specialization_courses')
        logging.info(f'Updating {table_name}')
        df_sc = df[df['contentType'] == 'Specialization'][['contentId', 'extraMetadata']].copy()
        df_sc['courseId'] = df_sc['extraMetadata'].apply(lambda x: extract_metadata(x))
        df_sc = df_sc.explode('courseId')
        df_sc = df_sc[['courseId', 'contentId']]
        df_sc = self.apply_data_types('specialization_courses', df_sc)
        df_sc.to_sql(table_name, con=self.engine, if_exists='replace', index=False)

        df.loc[df['contentType'] == 'Course',
               'estimatedLearningTime'] = df['extraMetadata'].apply(lambda x: extract_metadata(x))

        table_name, table_cols = self.get_table_params('contents')
        df_con = df[table_cols].copy()
        df_con = self.apply_data_types('contents', df_con)
        df_con.to_sql(table_name, con=self.engine, if_exists='replace', index=False)

    def update_user_changes(self):
        """
        One of the main functions.
        Calls other functions - creating tables, updating user table, updating contents if new ones found in enrolments
        :return: None, writes results to db
        """
        table_name, table_cols = self.get_table_params('enrolments_changes')
        logging.info(f'Updating {table_name}')
        self.check_tables()
        last_update_ts = self.get_last_update_ts(table_name)

        # get list of existing content
        with self.engine.connect() as connection:
            query = 'SELECT contentId FROM coursera_contents'
            existing_content_ids = [x[0] for x in connection.execute(query).fetchall()]

        enrolments_json = self.get_enrolments_json()
        self.update_enrolments(enrolments_json)

        # check if any changes
        df = pd.DataFrame(enrolments_json)
        df['last_update'] = datetime.utcnow()
        df = df[table_cols].copy()
        df = self.apply_data_types('enrolments_changes', df[table_cols])

        # if new contents in enrolments - update all content lists
        if not all(x in existing_content_ids for x in df['contentId']):
            self.update_contents()

        if last_update_ts is None:
            logging.info(f'No changes found, writing initial data')
            df.to_sql(table_name, con=self.engine, if_exists='replace', index=False)
        else:
            df = df[df['lastActivityAt'] > last_update_ts].copy()
            logging.info(f'Adding {df.shape[0]} record with updates from {last_update_ts}')
            df.to_sql(table_name, con=self.engine, index=False, if_exists='append')

    def update_scetl(self):
        """
        Start data update routines
        :return: None, calls functions that write results to db
        """
        self.update_user_changes()


'''
with open('configs/configs.json') as json_file:
    configs = json.load(json_file)

# engine = create_engine('mssql+pymssql://scetl:SemperInvicta90@localhost:1433/uchr')#
db_engine = create_engine(f'sqlite:///{os.getcwd()}/db3.sqlite')
logging.basicConfig(level=logging.INFO)


coursera_scetl = CourseraScetl(configs['coursera'], db_engine)
coursera_scetl.update_scetl()
eduson_scetl = EdusonScetl(configs['eduson'], db_engine)
eduson_scetl.update_scetl()
'''
