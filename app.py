import os
import json
import logging
from flask import Flask, render_template, redirect
from sqlalchemy import create_engine
from scetl import EdusonScetl, CourseraScetl


# engine = create_engine('mssql+pymssql://scetl:SemperInvicta90@localhost:1433/uchr')#
db_engine = create_engine(f'sqlite:///{os.getcwd()}/db.sqlite')
logging.basicConfig(level=logging.INFO)

app = Flask(__name__)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/update_scetls')
def update_scetls():
    with open('configs/configs.json') as json_file:
        configs = json.load(json_file)

    eduson = EdusonScetl(configs['eduson'], engine=db_engine)
    coursera = CourseraScetl(configs['coursera'], engine=db_engine)

    for hr_system in [eduson, coursera]:
        hr_system.update_scetl()
    return redirect('index')


if __name__ == '__main__':
    app.run()



