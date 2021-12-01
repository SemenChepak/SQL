import logging
from configparser import ConfigParser

import mysql.connector as sql
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

logging.basicConfig(filename='/opt/airflow/dags/SQLalchemy_task/logs/db_query_logs.log', filemode='w',
                    format='%(name)s - %(levelname)s - %(message)s')

config = ConfigParser()
config.read("/opt/airflow/dags/SQLalchemy_task/E_B/cred/cred.ini")

ENGINE = create_engine(f'mysql+mysqlconnector://{config.get("MySQL", "user")}:{config.get("MySQL", "password")}'
                       f'@host.docker.internal:{config.get("MySQL", "port")}/{config.get("MySQL", "database")}')

Session = sessionmaker(bind=ENGINE)
session = Session()


def select_all_person_from_db():
    """this method created to extract all row from persons.DB to create cards for them.
    Created because Airflow did not allow to extract data by means of SQLAlchemy"""

    db_connection = sql.connect(host='host.docker.internal', database=config.get("MySQL", "database"),
                                user=config.get("MySQL", "user"), password=config.get("MySQL", "password"))

    logging.warning(f'select_all_person_from_db Create connection to DB ')

    db_cursor = db_connection.cursor()

    db_cursor.execute('SELECT customer_id FROM sqlalchemy.people limit 1000;')

    logging.info('Extracting from DB')

    table_rows = db_cursor.fetchall()

    return pd.DataFrame(table_rows).to_dict()


def select_all_cards_from_db():
    """this method created to extract all row from cards.DB to create transactions for them.
    Created because Airflow did not allow to extract data by means of SQLAlchemy"""

    db_connection = sql.connect(host='host.docker.internal', database=config.get("MySQL", "database"),
                                user=config.get("MySQL", "user"), password=config.get("MySQL", "password"))

    logging.warning(f'select_all_cards_from_db Create connection to DB  ')

    db_cursor = db_connection.cursor()

    db_cursor.execute('SELECT card_no FROM sqlalchemy.cards limit 1000;')

    logging.info('Extracting from DB')

    table_rows = db_cursor.fetchall()

    return pd.DataFrame(table_rows).to_dict()
