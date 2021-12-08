import logging
import random

from sqlalchemy import create_engine

from packages.E_B.creds_getter import log_path, create_eng
from packages.SQLalchemy_task.Classes.main_classes import Cards, Transactions

logging.basicConfig(filename=f'{log_path()}/Transaction_worker_logs.log',
                    filemode='w',
                    format='%(name)s - %(levelname)s - %(message)s')

ENGINE = create_engine(create_eng())


def generate_transaction_insert(cards, session):
    logging.info(f"generate_transaction_insert get {len(card)} card")

    for card in cards:
        for j in range(random.randint(0, 5)):
            trans = Transactions()
            trans.card_number = card

            session.add(trans)

            session.query(Cards).\
                filter(Cards.card_no == card).\
                update({'last_used_on': trans.transaction_time})

            a = session.query(Cards.amount).\
                filter(Cards.card_no == card).one()

            session.query(Cards).\
                filter(Cards.card_no == card).\
                update({'amount': trans.value + a.amount})

    logging.info(" generate_transaction_insert insert into DB")

    session.commit()
