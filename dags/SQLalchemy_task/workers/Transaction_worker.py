import configparser
from random import choice

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from SQLalchemy_task.Classes.main_classes import Cards, Transactions

config = configparser.ConfigParser()
config.read("/opt/airflow/dags/SQLalchemy_task/E_B/cred/cred.ini")
ENGINE = create_engine(f'mysql+mysqlconnector://{config.get("MySQL", "user")}:{config.get("MySQL", "password")}'
                       f'@host.docker.internal:{config.get("MySQL", "port")}/{config.get("MySQL", "database")}')

Session = sessionmaker(bind=ENGINE)
session = Session()


def select_random_transaction_from_db():
    res = session.execute(select(Transactions))
    return choice(res.fetchall()[0])


def generate_transaction_insert(card: Cards):
    if card.is_active():
        trans = Transactions()
        trans.card_no = card.card_no
        session.query(Cards).filter(Cards.card_no == card.card_no).update({'last_used_on': trans.transaction_time})
        session.query(Cards).filter(Cards.card_no == card.card_no).update({'amount': card.amount + trans.value})
        session.add(trans)
        session.commit()
