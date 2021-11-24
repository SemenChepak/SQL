import configparser
from random import choice

from SQLalchemy_task.Classes.main_classes import People
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

config = configparser.ConfigParser()
config.read("/opt/airflow/dags/SQLalchemy_task/E_B/cred/cred.ini")
ENGINE = create_engine(f'mysql+mysqlconnector://{config.get("MySQL", "user")}:{config.get("MySQL", "password")}'
                       f'@host.docker.internal:{config.get("MySQL", "port")}/{config.get("MySQL", "database")}')

Session = sessionmaker(bind=ENGINE)
session = Session()


def select_random_person_from_db():
    res = session.execute(select(People))
    return choice(res.fetchall()[0])


def select_all_person_from_db():
    res = session.execute(select(People))
    return res


def generate_person_insert():
    person = People()

    return person
