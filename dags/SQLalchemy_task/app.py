import logging
from configparser import ConfigParser
from random import randint

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from SQLalchemy_task.Classes import main_classes
from SQLalchemy_task.db_worker.db_query import select_all_person_from_db, select_all_cards_from_db
from SQLalchemy_task.workers.Card_worker import generate_card_insert
from SQLalchemy_task.workers.Transaction_worker import generate_transaction_insert

logging.basicConfig(filename='/opt/airflow/dags/SQLalchemy_task/logs/app_logs.log', filemode='w',
                    format='%(name)s - %(levelname)s - %(message)s')

config = ConfigParser()
config.read("/opt/airflow/dags/SQLalchemy_task/E_B/cred/cred.ini")

ENGINE = create_engine(f'mysql+mysqlconnector://{config.get("MySQL", "user")}:{config.get("MySQL", "password")}'
                       f'@host.docker.internal:{config.get("MySQL", "port")}/{config.get("MySQL", "database")}')

Session = sessionmaker(bind=ENGINE)
session = Session()


def insert_person():
    for i in range(randint(1, 1000)):
        per = main_classes.People()
        session.add(per)
    session.commit()


def insert_card():
    persons_id_json = select_all_person_from_db()
    person = [*persons_id_json.values()][0]
    person_list = [*person.values()]
    generate_card_insert(person_list)


def insert_tr():
    card_id_json = select_all_cards_from_db()
    card = [*card_id_json.values()][0]
    card_list = [*card.values()]
    generate_transaction_insert(card_list)


def check_db():
    q = ENGINE.execute('SHOW TABLES').fetchall()
    if q == []:
        raise KeyError


def create_all():
    main_classes.Base.metadata.create_all(ENGINE)
