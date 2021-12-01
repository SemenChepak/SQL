import configparser
import logging
import random

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from SQLalchemy_task.Classes.main_classes import Cards, Transactions

logging.basicConfig(filename='/opt/airflow/dags/SQLalchemy_task/logs/Transaction_worker_logs.log', filemode='w',
                    format='%(name)s - %(levelname)s - %(message)s')

config = configparser.ConfigParser()
config.read("/opt/airflow/dags/SQLalchemy_task/E_B/cred/cred.ini")
ENGINE = create_engine(f'mysql+mysqlconnector://{config.get("MySQL", "user")}:{config.get("MySQL", "password")}'
                       f'@host.docker.internal:{config.get("MySQL", "port")}/{config.get("MySQL", "database")}')

Session = sessionmaker(bind=ENGINE)
session = Session()


def generate_transaction_insert(card):

    logging.info(f"generate_transaction_insert get {len(card)} card")

    for i in card:
        for j in range(random.randint(0, 5)):

            trans = Transactions()
            trans.card_number = i

            session.add(trans)

            session.query(Cards).filter(Cards.card_no == i).update({'last_used_on': trans.transaction_time})

            a = session.query(Cards.amount).filter(Cards.card_no == i).one()

            session.query(Cards).filter(Cards.card_no == i).update({'amount': trans.value + a.amount})

    logging.info(f" generate_transaction_insert insert into DB")

    session.commit()
