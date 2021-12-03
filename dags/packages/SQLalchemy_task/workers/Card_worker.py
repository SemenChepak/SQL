import logging
import random

from sqlalchemy import create_engine

from packages.E_B.creds_getter import log_path, create_eng
from packages.SQLalchemy_task.Classes.main_classes import Cards

logging.basicConfig(filename=f'{log_path()}/Card_worker_logs.log',
                    filemode='w',
                    format='%(name)s - %(levelname)s - %(message)s')

ENGINE = create_engine(create_eng())


def generate_card_insert(persons, session):
    logging.info(f"generate_card_insert get {len(persons)} persons")

    for person in persons:
        for j in range(random.randint(0, 1)):
            card = Cards()
            card.holder_id = person
            session.add(card)

    logging.info(" generate_card_insert insert into DB")

    session.commit()
