import logging
from random import randint

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from packages.E_B.creds_getter import log_path, create_eng
from packages.SQLalchemy_task.Classes import main_classes
from packages.SQLalchemy_task.db_worker.db_query import select_all_person_from_db, select_all_cards_from_db
from packages.SQLalchemy_task.workers.Card_worker import generate_card_insert
from packages.SQLalchemy_task.workers.Transaction_worker import generate_transaction_insert

logging.basicConfig(filename=f'{log_path()}/app_logs.log', filemode='w',
                    format='%(name)s - %(levelname)s - %(message)s')

ENGINE = create_engine(create_eng())


def insert_person():
    Session = sessionmaker(bind=ENGINE)
    session = Session()
    try:
        for i in range(randint(1, 1000)):
            per = main_classes.People()
            session.add(per)
        session.commit()
    except ConnectionError:
        session.rollback()
        raise
    finally:
        session.close()


def insert_card():
    Session = sessionmaker(bind=ENGINE)
    session = Session()
    try:
        persons_id_json = select_all_person_from_db()
        person = [*persons_id_json.values()][0]
        person_list = [*person.values()]
        generate_card_insert(person_list, session)
    except ConnectionError:
        session.rollback()
        raise
    finally:
        session.close()


def insert_tr():
    Session = sessionmaker(bind=ENGINE)
    session = Session()
    try:
        card_id_json = select_all_cards_from_db()
        card = [*card_id_json.values()][0]
        card_list = [*card.values()]
        generate_transaction_insert(card_list, session)
    except ConnectionError:
        session.rollback()
        raise
    finally:
        session.close()


def check_db():
    query = ENGINE.execute('SHOW TABLES').fetchall()
    if not query:
        raise KeyError


def create_all():
    main_classes.Base.metadata.create_all(ENGINE)
