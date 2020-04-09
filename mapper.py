import pandas as pd
from datetime import datetime
import os
import logging
from transliterate import translit


class EmployeeMapper:

    def __init__(self, engine):
        self.engine = engine

    @staticmethod
    def is_english(string):
        """
        checks if string in latin
        :param string: string
        :return: Boolean, True if latin
        """
        try:
            string.encode(encoding='utf-8').decode('ascii')
        except UnicodeDecodeError:
            return False
        else:
            return True

    def transliterate_latin(self, string):
        """
        transliterates string if it's latin
        :param string: any string
        :return: string transliterated to russian
        """
        if self.is_english(string):
            return translit(string, 'ru')
        else:
            return string

    def get_hr_df(self):
        """
        get table from v_hr_mapping view (relevant columns from HR table)
        preprocess it
        :return: preprocessed df
        """
        df = pd.read_sql('SELECT * FROM v_hr_mapping', con=self.engine)
        for col in ['email', 'employee_name']:
            df[col] = df[col].str.lower().str.replace('  ', ' ').str.split('(').str[0].str.strip()
        df['last_name'] = df['employee_name'].str.split(' ').str[0]
        df['first_name'] = df['employee_name'].str.split(' ').str[1]
        # 6 cases of ogly or kyzy patronyms - neglected them
        df['middle_name'] = df['employee_name'].str.split(' ').str[2]
        df['last_first_name'] = df['last_name'] + ' ' + df['first_name']
        for col in ['last_name', 'first_name', 'middle_name']:
            df[col] = df[col].fillna('')
        df['employee_name'] = df['last_name'] + ' ' + df['first_name'] + ' ' + df['middle_name']
        df['login'] = df['email'].str.split('@').str[0]
        return df

    def get_cloud_users_df(self):
        """
        get table from v_hr_cloud_users view (emails and names from cloud systems) and
        preprocess it
        :return: preprocessed df
        """
        df = pd.read_sql('SELECT * FROM v_hr_cloud_users', con=self.engine)
        df['email_clean'] = df['email'].str.lower().str.strip()
        df['login'] = df['email_clean'].str.split('@').str[0]
        for col in ['last_name', 'first_name', 'middle_name']:
            df[col] = df[col].fillna('')
            df[col] = df[col].str.lower().str.strip()
            df[col] = df[col].str.replace('none', '')
            df[col] = df[col].str.split(' ').str[0]
        df.loc[
            (df['last_name'] != '') & (df['first_name'] != ''),
            'last_first_name'
        ] = (df['last_name'] + ' ' + df['first_name']).str.strip()
        df['last_first_name'] = df['last_first_name'].fillna('')

        df.loc[
            (df['last_name'] != '') & (df['first_name'] != '') & (df['middle_name'] != ''),
            'full_name'
        ] = (df['last_name'] + ' ' + df['first_name'] + ' ' + df['middle_name']).str.strip()
        df['full_name'] = df['full_name'].fillna('')

        return df

    @staticmethod
    def get_employee_uid(df, mapping_type):
        """
        Checks if user can be identified by some parameter.
        :param df: filtered df_hr, result of filtering in identify_user call
        :param mapping_type: filtering attribute in identify_user call
        :return: list [employee_id, mapping_method, mapping_type]
        """
        if df.shape[0] == 0:
            # if empty df then keep looking
            return ['not_mapped', 'not_mapped', mapping_type]
        elif df.shape[0] == 1:
            # if only one employee with this mail then select him
            return [df['employee_uid'].iloc[0], 'only_choice', mapping_type]
        else:
            # else try to find by exit date
            # in the sql view all active employees have exit date of 2100-12-31
            # so if only one employee works then pick him. Else the latest exited
            max_exit = df['exit_date'].max()
            df = df[df['exit_date'] == max_exit]
            if df.shape[0] == 1:
                return [df['employee_uid'].iloc[0], 'only_active', mapping_type]
            else:
                # one person an work several jobs
                # main work for this person is typically has 'ВидЗанятости' == True
                # in sql view this column is replaced by main_workplace
                df = df[df['main_workplace'] == True]
                if df.shape[0] == 1:
                    return [df['employee_uid'].iloc[0], 'only_main', mapping_type]
                else:
                    # else return possible options for user uid
                    possible_options = list(df['employee_uid'])
                    return [possible_options, 'needs_manual', mapping_type]

    def identify_user(self, row, df_hr):
        """
        Trys to identify user by columns specified in attempts dict
        Applys get_employee_id function to dataframes filtered by row params
        :param row: row of dataframe in map_users call
        :param df_hr: result of get_hr_df call (HR table from DWH)
        :return: list of dicts with mapping results
        """
        result_dict = {'hr_system': row.hr_system, 'system_email': row.email}
        attempts_dict = {
            'email': row.email_clean,
            'employee_name': row.full_name,
            'last_first_name': self.transliterate_latin(row.last_first_name),
            'login': row.login,
        }
        for param in attempts_dict.keys():
            mapping_result = self.get_employee_uid(
                df_hr[df_hr[param] == attempts_dict[param]],
                param)
            if mapping_result[0] != 'not_mapped':
                result_dict['employee_id'] = mapping_result[0]
                result_dict['mapping_method'] = mapping_result[1]
                result_dict['mapping_source'] = mapping_result[2]
                return result_dict
        return {'hr_system': row.hr_system, 'system_email': row.email, 'employee_id': ['no_options'],
                'mapping_method': 'not_mapped', 'mapping_source': 'not_mapped'}

    def map_users(self):
        """
        Compares all known users from cloud system and hr table (+ manual mapped table)
        If new cases - maps them and writes resulting employee id to hr_cloud_mapped_auto table
        all not mapped users writes to hr_cloud_mapping_needed table
        :return: None, writes to db
        """
        df = self.get_cloud_users_df()
        cases_total = df.shape[0]
        if self.engine.dialect.has_table(self.engine, 'v_hr_cloud_mapping'):
            df_known = pd.read_sql('SELECT * FROM v_hr_cloud_mapping', self.engine)
            mapped_emails = df_known['system_email'].unique()
            df = df[~df['email'].isin(mapped_emails)]
        df_hr = self.get_hr_df()

        # God of python please forgive me for iterating over data frame :(
        mapped_records = []
        manual_mapping_needed = []
        for row in df.itertuples():
            identity = self.identify_user(row, df_hr)
            if identity['mapping_method'] == 'needs_manual' or identity['mapping_method'] == 'not_mapped':
                manual_mapping_needed.append(identity)
            else:
                mapped_records.append(self.identify_user(row, df_hr))
        logging.info(f'{len(manual_mapping_needed)} / {cases_total} users not mapped.')
        df_map = pd.DataFrame(mapped_records)
        df_map['last_update'] = datetime.utcnow()
        df_map.to_sql('hr_cloud_mapped_auto', con=self.engine, index=False, if_exists='append')
        df_map_needed = pd.DataFrame(manual_mapping_needed)
        df_map_needed = df_map_needed.explode('employee_id')
        df_map_needed.rename(columns={'employee_id': 'possible_options'}, inplace=True)
        if self.engine.dialect.has_table(self.engine, 'hr_cloud_mapping_needed'):
            df_known_not_mapped = pd.read_sql('SELECT * FROM hr_cloud_mapping_needed', con=self.engine)
            known_not_mapped_emails = df_known_not_mapped['system_email'].unique()
            logging.info(f'{len(known_not_mapped_emails)} / {df_map_needed.system_email.nunique()} already checked')
            df_map_needed.loc[df_map_needed['system_email'].isin(known_not_mapped_emails), 'already_mapped'] = 1
            df_map_needed['already_mapped'] = df_map_needed['already_mapped'].fillna(0)
            logging.info(f'{df_map_needed[df_map_needed.already_mapped == 0].shape[0]} are still needed to check')
        df_map_needed.to_sql('hr_cloud_mapping_needed', con=self.engine, index=False, if_exists='replace')
        if df_map_needed.shape[0] > 0:
            if not os.path.exists('support'):
                os.mkdir('support')
            excel_path = 'support/for_manual_mapping.xlsx'
            logging.info(f'Saved users that need mapping to {excel_path}')
            df_map_needed.to_excel('support/for_manual_mapping.xlsx', index=False)

    def update_manual_from_excel(self):
        """
        updates manual_mapping table provided in excel...
        :return: None, writes to db
        """
        excel_path = 'support/manual_mapping.xlsx'
        if not os.path.isfile(excel_path):
            logging.info(f'could not load file since {excel_path} does not exist')
        else:
            df_map_man = pd.read_excel(excel_path, sheet_name='final', skiprows=1)
            df_map_man = df_map_man[df_map_man['load_to_manual'] == 1]
            df_map_man['last_update'] = datetime.utcnow()
            df_map_man.to_sql('hr_cloud_mapped_manual', con=self.engine, if_exists='replace', index=False)
            logging.info(f'Uploaded {df_map_man.shape[0]} rows to manual mapping')
