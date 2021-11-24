import configparser
from random import choice

from SQLalchemy_task.Classes.main_classes import Cards, Transactions
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

config = configparser.ConfigParser()
config.read("/opt/airflow/dags/SQLalchemy_task/E_B/cred/cred.ini")
ENGINE = create_engine(f'mysql+mysqlconnector://{config.get("MySQL", "user")}:{config.get("MySQL", "password")}'
                       f'@host.docker.internal:{config.get("MySQL", "port")}/{config.get("MySQL", "database")}')

Session = sessionmaker(bind=ENGINE)
session = Session()


def select_random_transaction_from_db():
    res = session.execute(select(Transactions))
    return choice(res.fetchall()[0])


def generate_transaction_insert(card):
    trans = Transactions()
    trans.card_no = int(card)
    # session.query(Cards).filter(Cards.card_no == int(card)).update({'last_used_on': trans.transaction_time})
    # session.query(Cards).filter(Cards.card_no == int(card)).update({'amount': trans.value})
    return trans
