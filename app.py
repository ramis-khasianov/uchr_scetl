import os
import json
import logging
import schedule
import time
import pandas as pd
from sqlalchemy import create_engine
from scetl import EdusonScetl, CourseraScetl, AssessFirstScetl, SkillazScetl

ms_db_engine = create_engine('mssql+pymssql://scetl:SemperInvicta90@localhost:1433/uchr')
db_engine = create_engine(f'sqlite:///{os.getcwd()}/db.sqlite')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def start_updates():
    """
    Makes scetl instances from config file and start corresponding updates
    :return:
    """
    logging.info('Starting sceetl updater')
    with open('configs/configs.json') as json_file:
        configs = json.load(json_file)

    eduson_scetl = EdusonScetl(configs['eduson'], db_engine)
    coursera_scetl = CourseraScetl(configs['coursera'], db_engine)
    skillaz_scetl = SkillazScetl(configs['skillaz'], db_engine)
    assess_first_scetl = AssessFirstScetl(configs['assess_first'], db_engine)

    eduson_scetl.update_scetl()
    coursera_scetl.update_scetl()
    skillaz_scetl.update_scetl()
    assess_first_scetl.update_scetl()


def make_csv_files():
    """
    Make backups in csv
    :return: None, makes files on function call
    """
    logging.info('Starting csv generator')
    with open('configs/configs.json') as json_file:
        configs = json.load(json_file)

    if not os.path.exists('csv_files/'):
        os.mkdir('csv_files/')

    for hr_system in configs:
        for table in configs[hr_system]['tables']:
            table_name = configs[hr_system]['tables'][table]['table_name']
            logging.info(f'Coping table {table_name} to csv')
            df = pd.read_sql_table(table_name, con=db_engine)
            df.to_csv('csv_files/' + table_name + '.csv', index=False)


def copy_to_sql_server():
    """
    Copy from local sqlite to sql server
    :return: None, writes to database
    """
    logging.info(f'Copying to mssql server')
    with open('configs/configs.json') as json_file:
        configs = json.load(json_file)

    for hr_system in configs:
        for table in configs[hr_system]['tables']:
            table_name = configs[hr_system]['tables'][table]['table_name']
            logging.info(f'Coping table {table_name} to sql server')
            df = pd.read_sql_table(table_name, con=db_engine)
            df.to_sql('hr.' + table_name, con=ms_db_engine, index=False, if_exists='replace')
    logging.info(f'Done with copying')


def check_if_update_on_start():
    """
    While testing starting specific routines required - helper function
    :return: True if user requested update
    """
    user_input = input('Start update now? (y/n): ')
    if user_input[0].lower() == 'y':
        with open('configs/configs.json') as json_file:
            configs = json.load(json_file)
        user_input_system = input('Which ones? (all/eduson/coursera/skillaz/af/none): ')
        if user_input_system == 'all':
            start_updates()
        if user_input_system == 'eduson':
            eduson_scetl = EdusonScetl(configs['eduson'], db_engine)
            eduson_scetl.update_scetl()
        if user_input_system == 'coursera':
            coursera_scetl = CourseraScetl(configs['coursera'], db_engine)
            coursera_scetl.update_scetl()
        if user_input_system == 'skillaz':
            skillaz_scetl = SkillazScetl(configs['skillaz'], db_engine)
            skillaz_scetl.update_scetl()
        if user_input_system == 'af':
            assess_first_scetl = AssessFirstScetl(configs['assess_first'], db_engine)
            assess_first_scetl.update_scetl()
        if_make_csv = input('Make csvs? (y/n): ')
        if if_make_csv[0].lower() == 'y':
            make_csv_files()
        if_write_mssql = input('Copy data to sql? (y/n): ')
        if if_write_mssql[0].lower() == 'y':
            copy_to_sql_server()
        return True
    else:
        return False


# Jobs scheduled
schedule.every().day.at("21:45").do(start_updates)
schedule.every().day.at("22:00").do(make_csv_files)
schedule.every().day.at("22:10").do(copy_to_sql_server)

check_if_update_on_start()

while True:
    schedule.run_pending()
    time.sleep(1)

