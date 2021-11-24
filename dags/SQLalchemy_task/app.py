from configparser import ConfigParser
from random import randint

from SQLalchemy_task.workers.Card_worker import generate_card_insert
from SQLalchemy_task.workers.People_worker import generate_person_insert
from SQLalchemy_task.workers.Transaction_worker import generate_transaction_insert
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

config = ConfigParser()
config.read("/opt/airflow/dags/SQLalchemy_task/E_B/cred/cred.ini")

ENGINE = create_engine(f'mysql+mysqlconnector://{config.get("MySQL", "user")}:{config.get("MySQL", "password")}'
                       f'@host.docker.internal:{config.get("MySQL", "port")}/{config.get("MySQL", "database")}')

Session = sessionmaker(bind=ENGINE)
session = Session()


def insert():
    for i in range(randint(1, 1000)):
        per = generate_person_insert()
        for j in range(randint(0, 1)):
            card = generate_card_insert(per)
            for k in range(randint(0, 5)):
                generate_transaction_insert(card)



