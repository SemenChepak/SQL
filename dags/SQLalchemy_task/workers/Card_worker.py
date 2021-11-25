import configparser
import random

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from SQLalchemy_task.Classes.main_classes import Cards

config = configparser.ConfigParser()
config.read("/opt/airflow/dags/SQLalchemy_task/E_B/cred/cred.ini")

ENGINE = create_engine(f'mysql+mysqlconnector://{config.get("MySQL", "user")}:{config.get("MySQL", "password")}'
                       f'@host.docker.internal:{config.get("MySQL", "port")}/{config.get("MySQL", "database")}')

Session = sessionmaker(bind=ENGINE)
session = Session()


def generate_card_insert(persons):
    for i in persons:
        for j in range(random.randint(0, 1)):
            card = Cards()
            card.holder_id = i
            session.add(card)
    session.commit()
