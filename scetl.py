import os
import json
import requests
import logging
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, DateTime, String, Float, Text

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
        'UNIXTIME_MS': DateTime,
        'UNIXTIME_S': DateTime,
        'TEXT': Text
    }

    # Data type to convert pandas data frames
    pd_data_types = {
        'INT': 'int',
        'VARCHAR': 'str',
        'NUMERIC': 'float',
        'DATETIME': 'datetime',
        'UNIXTIME_MS': 'unixtime_ms',
        'UNIXTIME_S': 'unixtime_s',
        'TEXT': 'str'
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

    def add_missing_columns(self, table, df):
        """
        Adds empty columns to pd.DataFrame in case api structure changes (Currently (03.04.20) the case for skillaz)
        :param table: table dict from config dict (not table_name)
        :param df: DataFrame to process, usually df made from api call json
        :return: same df with proper data types
        """
        cols = self.config['tables'][table]['columns']
        cols_list = [x['name'] for x in cols]
        absent_columns = [x for x in cols_list if x not in df.columns]
        if absent_columns:
            logging.info(f'Not found columns in {table} response: {absent_columns}')
            for col in absent_columns:
                df[col] = None
        return df

    def apply_data_types(self, table, df):
        """
        Transforms pd.DataFrame data types in accordance with schema provided in config-tables
        :param table: table dict from config dict (not table_name)
        :param df: DataFrame to process, usually df made from api call json
        :return: same df with proper data types
        """
        cols = self.config['tables'][table]['columns']
        col_dict = {x['name']: self.pd_data_types[x['type']] for x in cols}
        df = df.copy()
        for col in df.columns:
            if col_dict[col] == 'datetime':
                df[col] = pd.to_datetime(df[col], yearfirst=True)
                df[col] = df[col].dt.tz_localize(None)
            elif col_dict[col] == 'unixtime_ms':
                df[col] = pd.to_datetime(df[col], unit='ms')
            elif col_dict[col] == 'unixtime_s':
                df[col] = pd.to_datetime(df[col], unit='s')
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

    def update_user_courses_changes(self, user_id, user_courses_json=None):
        """
        Adds record of course's current progress if user user had any activity since last update
        :param user_id: user id for Eduson
        :param user_courses_json: results from get_user_courses_json call
        :return: None, writes results to db
        """
        table_name, table_cols = self.get_table_params('user_courses_changes')
        if user_courses_json is None:
            user_courses_json = self.get_user_courses_json(user_id)
        df = pd.DataFrame(user_courses_json['courses'])
        df['last_update'] = datetime.utcnow()
        df['user_id'] = user_id
        df = df[table_cols]
        df = self.apply_data_types('user_courses_changes', df)
        df.to_sql(table_name, con=self.engine, if_exists='append', index=False)

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
            self.update_user_courses_changes(user_id, response)

    def update_user_changes(self):
        """
        One of the main functions.
        Calls other functions - creating tables, updating user table, updating courses for users that have changes
        :return: None, writes results to db
        """
        table_name, table_cols = self.get_table_params('user_changes')

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
        Check if tables exist and start data update routines
        :return: None, calls functions that write results to db
        """
        self.check_tables()
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

    def get_memberships_json(self):
        """
        Calls get_paged_json function for memberships url
        :return: list, result of get_paged_json
        """
        return self.get_paged_json(self.urls['memberships']['url'])

    def get_invitations_json(self):
        """
        Calls get_paged_json function for invitations url
        :return: list, result of get_paged_json
        """
        return self.get_paged_json(self.urls['invitations']['url'])

    def update_invitations(self):
        """
        Clean and save results of get_invitations_json to database
        :return: None, writes results to db
        """
        table_name, table_cols = self.get_table_params('invitations')
        invitations_json = self.get_invitations_json()
        df = pd.DataFrame(invitations_json)
        df['last_update'] = datetime.utcnow()
        df = self.apply_data_types('invitations', df[table_cols])
        df.to_sql(table_name, con=self.engine, if_exists='replace', index=False)

    def update_memberships(self):
        """
        Clean and save results of get_memberships_json to database
        :return: None, writes results to db
        """
        table_name, table_cols = self.get_table_params('memberships')
        memberships_json = self.get_memberships_json()
        df = pd.DataFrame(memberships_json)
        df['last_update'] = datetime.utcnow()
        df = self.apply_data_types('memberships', df[table_cols])
        df.to_sql(table_name, con=self.engine, if_exists='replace', index=False)

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
        Check if tables exist and start data update routines
        :return: None, calls functions that write results to db
        """
        self.check_tables()
        self.update_memberships()
        self.update_invitations()
        self.update_user_changes()


# -----------------
# ---AssessFirst---
# -----------------


class AssessFirstScetl(Scetl):

    # all candidates in assess first are accessible only for specific user.
    # each user has his own token and his candidates are only accessible with his token
    # so all methods have user as parameter, so that api call can get api token for this user

    def get_current_candidates_statuses(self):
        """
        Get finished assessments per candidate. So we won't call results for finished candidates
        :return: python dict where key is uuid and value is number of finished assessments
        """
        with self.engine.connect() as connection:
            query = '''
                SELECT uuid, COUNT(*) AS finished_assessments 
                FROM (SELECT DISTINCT uuid, name, status FROM assess_first_assessments)
                WHERE status = 'finish' GROUP BY uuid        
            '''
            current_candidates_statuses = {x[0]: x[1] for x in connection.execute(query).fetchall()}
        return current_candidates_statuses

    def get_candidates_json(self, user, page=None):
        """
        Get list of candidates accessible for this user
        :param user: user to get his token for request headers
        :param page: page as param for api call (max 50 candidates per api call)
        :return: python dict - result of the call
        """
        token = self.config['users'][user]['token']
        header_name = self.config['request_headers']['header_name']
        url = self.urls['candidates_list']['url']
        params = self.urls['candidates_list']['params']
        if page is not None:
            params['page'] = page
        logging.info(f'Updating candidate page {page}: calling {url} with params {params}')
        response = requests.get(url, headers={header_name: token}, params=params).json()
        return response

    def get_paginated_candidates_json(self, user):
        """
        Get results of get_candidates_json call as paginated dict
        :param user: user to get his token for request headers
        :return: python dict with pages as keys and results of get_candidates_json as values
        """
        page = 0
        last_page = 1
        paged_response = {}
        while page != int(last_page):

            page += 1
            candidates_json = self.get_candidates_json(user, page)
            paged_response[f'page_{page}'] = candidates_json
            last_page = candidates_json['meta']['last_page']

        concat_response = [item for x in paged_response.keys() for item in paged_response[x]['data']]
        return concat_response

    def get_results_json(self, user, uuid):
        """
        Get results of candidate/results call as python dict
        :param user: user to get his token for request headers
        :param uuid: candidate uuid - need as path variable to api call
        :return: python dict - result of the call
        """
        token = self.config['users'][user]['token']
        header_name = self.config['request_headers']['header_name']
        url = self.urls['candidate_results']['url'].replace('{uuid}', uuid)
        params = self.urls['candidate_results']['params']
        response = requests.get(url, headers={header_name: token}, params=params).json()
        return response

    def get_synthesis_json(self, user, uuid, candidate_token=None):
        """
        Get results of candidate/synthesis call as python dict
        :param user: user to get his token for request headers
        :param uuid: candidate uuid - needed to get token from get_results_json if no candidate token provided
        :param candidate_token: candidate token (gotten from results call) - need as path variable to api call
        :return: python dict - result of the call
        """
        if candidate_token is None:
            candidate_token = self.get_results_json(user, uuid)['token']
        token = self.config['users'][user]['token']
        header_name = self.config['request_headers']['header_name']
        url = self.urls['candidate_synthesis']['url'].replace('{token}', candidate_token)
        params = self.urls['candidate_synthesis']['params']
        response = requests.get(url, headers={header_name: token}, params=params).json()
        return response

    @staticmethod
    def parse_synthesis_json(synthesis_json):
        """
        Synthesis call results are quite a unstructured mess.
        This static methods parses results to a more agreable format
        :param synthesis_json: results of get_synthesis_json
        :return: list of records from synthesis call
        """
        results_list = []
        for block in synthesis_json:
            if synthesis_json[block] is not None:
                for param in synthesis_json[block]:
                    if type(synthesis_json[block][param]) is list:
                        for list_item in synthesis_json[block][param]:
                            record = {
                                'block': block,
                                'item': param,
                                'value': list_item,
                                'additional_value': None}
                            results_list.append(record)
                    elif type(synthesis_json[block][param]) is str:
                        record = {
                            'block': block,
                            'item': param,
                            'value': synthesis_json[block][param],
                            'additional_value': None
                        }
                        results_list.append(record)
                    elif param in ['bad_squares', 'good_squares']:
                        for square in synthesis_json[block][param].keys():
                            record = {
                                'block': block,
                                'item': param,
                                'value': synthesis_json[block][param][square]['label'],
                                'additional_value': square
                            }
                            results_list.append(record)
                    elif param in ['privileged', 'decision', 'learning']:
                        record = {
                            'block': block,
                            'item': param,
                            'value': synthesis_json[block][param]['value'],
                            'additional_value': synthesis_json[block][param]['description']
                        }
                        results_list.append(record)
                    else:
                        record = {'block': block, 'item': param, 'value': None, 'additional_value': None}
                        results_list.append(record)

        return results_list

    def update_candidate_result(self, user, uuid, results_json=None):
        """
        Updates candidate's results in database if there are any changes in finished assessments
        :param user: user to get his token for request headers
        :param uuid: candidate uuid - need as path variable to api call
        :param results_json: results of get_results_json so that call is not made second time
        :return: None, writes results to db
        """
        if results_json is None:
            results_json = self.get_results_json(user, uuid)
        logging.info(f'Updating results for user {user}, candidate {uuid}')
        with self.engine.connect() as connection:
            query = f"DELETE FROM assess_first_assessments WHERE uuid = '{uuid}'"
            connection.execute(query)

        table_name, table_cols = self.get_table_params('assessments')
        df_assessments = pd.DataFrame(results_json['assessments'])
        df_assessments['last_update'] = datetime.utcnow()
        df_assessments['uuid'] = uuid
        df_assessments = self.apply_data_types('assessments', df_assessments[table_cols])
        df_assessments.to_sql(table_name, con=self.engine, if_exists='append', index=False)

        if 'finish' in list(df_assessments['status']):
            with self.engine.connect() as connection:
                query = f"DELETE FROM assess_first_results WHERE uuid = '{uuid}'"
                connection.execute(query)
            table_name, table_cols = self.get_table_params('results')
            df_results = pd.DataFrame(results_json['results'])
            df_results['last_update'] = datetime.utcnow()
            df_results['uuid'] = uuid
            df_results = self.apply_data_types('results', df_results[table_cols])
            df_results.to_sql(table_name, con=self.engine, if_exists='append', index=False)

    def update_candidate_synthesis(self, user, uuid, results_json=None):
        """
        Updates candidate's synthesis in database if there are any changes in finished assessments
        :param user: user to get his token for request headers
        :param uuid: candidate uuid - need as path variable to api call
        :param results_json: results of get_results_json so that call is not made second time
        :return: None, writes results to db
        """
        if results_json is None:
            candidate_token = None
        else:
            candidate_token = results_json['token']
        logging.info(f'Updating synthesis for user {user}, candidate {uuid}')
        synthesis_json = self.get_synthesis_json(user, uuid, candidate_token)
        results_list = self.parse_synthesis_json(synthesis_json)

        with self.engine.connect() as connection:
            query = f"DELETE FROM assess_first_synthesises WHERE uuid = '{uuid}'"
            connection.execute(query)

        table_name, table_cols = self.get_table_params('synthesises')
        df = pd.DataFrame(results_list)
        df['last_update'] = datetime.utcnow()
        df['uuid'] = uuid
        df = self.apply_data_types('synthesises', df[table_cols])
        df.to_sql(table_name, con=self.engine, if_exists='append', index=False)

    def update_candidates(self):
        """
        Main method. Gets list of candidates from db with finished assessments.
        Then goes to cycle through all users, for each of them get list of candidates and if they have below
        3 finished assessments - makes results call. If candidate has new finished assessments -
        it makes result and synthesis calls, updating data in db
        :return: None, writes results to db
        """
        users = self.config['users']
        for user in users:
            current_candidates_statuses = self.get_current_candidates_statuses()
            finished_candidates = [
                x for x in current_candidates_statuses.keys() if current_candidates_statuses[x] >= 3
            ]
            logging.info(f'updating candidates for {user}')
            candidates = self.get_paginated_candidates_json(user)

            table_name, table_cols = self.get_table_params('candidates')
            with self.engine.connect() as connection:
                query = f"DELETE FROM assess_first_candidates WHERE owner = '{user}'"
                connection.execute(query)
            df_candidates = pd.DataFrame(candidates)
            df_candidates['last_update'] = datetime.utcnow()
            df_candidates['owner'] = user
            df_candidates = self.apply_data_types('candidates', df_candidates[table_cols])
            df_candidates.to_sql(table_name, con=self.engine, if_exists='append', index=False)

            not_finished_candidates = [x for x in df_candidates['uuid'] if x not in finished_candidates]
            logging.info(f'Found {len(not_finished_candidates)} not finished candidates. '
                         f'Total candidates: {df_candidates.shape[0]}')
            current_candidate = 0

            for uuid in not_finished_candidates:
                current_candidate += 1
                logging.info(f'Updating candidate {current_candidate} out of {len(not_finished_candidates)}')
                results_json = self.get_results_json(user, uuid)
                finished_assessments = len(
                    [x['name'] for x in results_json['assessments'] if x['status'] == 'finish']
                )
                try:
                    current_known_assessments = current_candidates_statuses[uuid]
                except KeyError:
                    current_known_assessments = 0
                logging.info(f'Updating user {user}, candidate {uuid}. '
                             f'Known assessments: {current_known_assessments}. '
                             f'Discovered assessments: {finished_assessments}')
                if finished_assessments > current_known_assessments:
                    self.update_candidate_result(user, uuid, results_json)
                    self.update_candidate_synthesis(user, uuid, results_json)

    def update_scetl(self):
        """
        Check if tables exist and start data update routines
        :return: None, calls functions that write results to db
        """
        self.check_tables()
        self.update_candidates()


# -------------
# ---Skillaz---
# -------------


class SkillazScetl(Scetl):

    # Skillaz is bound to be updated as system is not finished as of 29.03.2020

    def get_skillaz_json(self, url):
        """
        Make api call and return results as python dict
        :param url: api endpoint to call
        :return: results of api call as python dict
        """
        headers = {
            self.config['request_headers']['header_name']: self.config['request_headers']['header_value']
        }
        url = self.urls[url]['url']
        response = requests.get(url, headers=headers).json()
        return response

    @staticmethod
    def parse_skillaz_response(response_json, json_type):
        """
        Parses data from candidates, requests, offers api calls to cleaner format
        :param response_json: python dict, result from get_skillaz_json call for corresponding url
        :param json_type: name of the call (candidates, requests or offers)
        :return: python list of records for a call
        """
        main_data = []
        workflow_data = []
        for item in response_json['Items']:
            main_data_row = item['Data']
            workflow = item['Workflow']['States']
            for wf in workflow:
                wf['Schema'] = item['Workflow']['Schema']
                wf['Id'] = item['Id']
                if json_type == 'candidates':
                    wf['VacancyId'] = item['VacancyId']
                    wf['RequestId'] = item['RequestId']
                if json_type == 'requests':
                    wf['VacancyId'] = item['VacancyId']
                if json_type == 'offers':
                    wf['CandidateId'] = item['CandidateId']
            workflow_data = workflow_data + workflow
            main_data_row['Id'] = item['Id']
            if json_type == 'candidates':
                main_data_row['RequestId'] = item['RequestId']
            if json_type == 'request':
                main_data_row['Name'] = item['Name']
            if json_type == 'offer':
                main_data_row['CandidateId'] = item['CandidateId']
            for i in item['Audit']:
                main_data_row[i] = item['Audit'][i]
            main_data.append(main_data_row)
        return main_data, workflow_data

    @staticmethod
    def parse_vacancies(vacancies_json):
        """
        Parses data from vacancies api call to cleaner format
        :param vacancies_json: python dict, result from get_skillaz_json call for vacancies url
        :return: python list of vacancies records
        """
        vacancies = []
        for item in vacancies_json['Items']:
            vacancy_data = item['Data']
            vacancy_data['Id'] = item['Id']
            vacancy_data['Name'] = item['Name']
            vacancy_data['IsActive'] = item['IsActive']
            for i in item['Audit']:
                vacancy_data[i] = item['Audit'][i]
            vacancies.append(vacancy_data)
        return vacancies

    def update_vacancies(self):
        """
        Makes vacancies api call and saves data to db
        :return: None, writes results to db
        """
        logging.info(f'Updating table vacancies')
        vacancies_json = self.get_skillaz_json('vacancies')
        df = pd.DataFrame(self.parse_vacancies(vacancies_json))
        df['last_update'] = datetime.utcnow()
        table_name, table_cols = self.get_table_params('vacancies')
        new_columns = [x for x in df.columns if x not in table_cols]
        if new_columns:
            logging.info(f'New columns in vacancies response: {new_columns}')
        df = self.add_missing_columns('vacancies', df)
        df = self.apply_data_types('vacancies', df[table_cols])
        df.to_sql(table_name, con=self.engine, if_exists='replace', index=False)

    def update_skillaz(self):
        """
        Cycle through three api calls from skillaz and save current data from them in six table:
        for each call there is a data table and workflow table
        :return: None, writes results to db
        """
        for json_type in ['candidates', 'offers', 'requests']:
            response = self.get_skillaz_json(json_type)
            data_json, workflow_json = self.parse_skillaz_response(response, json_type)
            jsons_dict = {
                json_type: data_json,
                json_type + '_workflow': workflow_json
            }
            for table in jsons_dict:
                logging.info(f'Updating table {table}')
                df = pd.DataFrame(jsons_dict[table])
                df['last_update'] = datetime.utcnow()
                table_name, table_cols = self.get_table_params(table)
                new_columns = [x for x in df.columns if x not in table_cols]
                if new_columns:
                    logging.info(f'New columns in {table} response: {new_columns}')
                df = self.add_missing_columns(table, df)
                df = self.apply_data_types(table, df[table_cols])
                df.to_sql(table_name, con=self.engine, if_exists='replace', index=False)

    def update_scetl(self):
        """
        Check if tables exist and start data update routines
        :return: None, calls functions that write results to db
        """
        self.check_tables()
        self.update_vacancies()
        self.update_skillaz()
