from configparser import ConfigParser
from random import randint

from SQLalchemy_task.Classes import main_classes
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

config = ConfigParser()
config.read("/opt/airflow/dags/SQLalchemy_task/E_B/cred/cred.ini")

ENGINE = create_engine(f'mysql+mysqlconnector://{config.get("MySQL", "user")}:{config.get("MySQL", "password")}'
                       f'@host.docker.internal:{config.get("MySQL", "port")}/{config.get("MySQL", "database")}')

Session = sessionmaker(bind=ENGINE)
session = Session()


def insert():
    persons = []
    cards = []
    for i in range(randint(1, 1000)):
        per = main_classes.People()
        session.add(per)
        session.commit()

        for i in range(randint(1, 2)):
            card = main_classes.Cards()
            card.holder_id = per.customer_id
            session.add(card)
            session.commit()

            for j in range(randint(0, 5)):
                tr = main_classes.Transactions()
                tr.card_no = card.card_no
                session.query(main_classes.Cards).filter(main_classes.Cards.card_no == int(card.card_no)).update(
                    {'last_used_on': tr.transaction_time})
                session.query(main_classes.Cards).filter(main_classes.Cards.card_no == int(card.card_no)).update(
                    {'amount': tr.value})

                session.add(tr)
                session.commit()


def check_db():
    q = ENGINE.execute('SHOW TABLES').fetchall()
    if q == []:
        raise KeyError


def create_all():
    main_classes.Base.metadata.create_all(ENGINE)
