import os
import json
import logging
import schedule
import time
import pandas as pd
from sqlalchemy import create_engine
from scetl import EdusonScetl, CourseraScetl, AssessFirstScetl, SkillazScetl

# db_engine = create_engine('mssql+pymssql://scetl:SemperInvicta90@localhost:1433/uchr')
db_engine = create_engine(f'sqlite:///{os.getcwd()}/db.sqlite')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def start_updates():
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


schedule.every().day.at("21:45").do(start_updates)
schedule.every().day.at("22:00").do(make_csv_files)

while True:
    schedule.run_pending()
    time.sleep(1)

