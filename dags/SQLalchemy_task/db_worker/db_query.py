from configparser import ConfigParser

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

config = ConfigParser()
config.read("/opt/airflow/dags/SQLalchemy_task/E_B/cred/cred.ini")

ENGINE = create_engine(f'mysql+mysqlconnector://{config.get("MySQL", "user")}:{config.get("MySQL", "password")}'
                       f'@host.docker.internal:{config.get("MySQL", "port")}/{config.get("MySQL", "database")}')

Session = sessionmaker(bind=ENGINE)
session = Session()


def select_all_person_from_db():
    import mysql.connector as sql
    db_connection = sql.connect(host='host.docker.internal', database=config.get("MySQL", "database"),
                                user=config.get("MySQL", "user"), password=config.get("MySQL", "password"))
    db_cursor = db_connection.cursor()
    db_cursor.execute('SELECT customer_id FROM sqlalchemy.people;')

    table_rows = db_cursor.fetchall()

    return pd.DataFrame(table_rows).to_dict()


def select_all_cards_from_db():
    import mysql.connector as sql
    db_connection = sql.connect(host='host.docker.internal', database=config.get("MySQL", "database"),
                                user=config.get("MySQL", "user"), password=config.get("MySQL", "password"))
    db_cursor = db_connection.cursor()
    db_cursor.execute('SELECT card_no FROM sqlalchemy.cards;')

    table_rows = db_cursor.fetchall()

    return pd.DataFrame(table_rows).to_dict()
