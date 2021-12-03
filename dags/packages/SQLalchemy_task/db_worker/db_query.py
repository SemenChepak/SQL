import logging

import mysql.connector as sql
import pandas as pd

from packages.E_B.creds_getter import get_creds_sql, log_path, get_sql

Query = get_sql()
SQL_CR = get_creds_sql()

logging.basicConfig(filename=f'{log_path()}/db_query_logs.log', filemode='w',
                    format='%(name)s - %(levelname)s - %(message)s')


def select_all_person_from_db():
    """this method created to extract all row from persons.DB
     to create cards for them.
    Created because Airflow did not allow to
    extract data by means of SQLAlchemy
    """
    db_connection = sql.connect(host=SQL_CR["host"],
                                database=SQL_CR["database"],
                                user=SQL_CR['user'],
                                password=SQL_CR["password"]
                                )
    try:

        logging.warning('select_all_person_from_db Create connection to DB')

        db_cursor = db_connection.cursor()

        db_cursor.execute(Query['select_customer_id'])

        logging.info('Extracting from DB')

        table_rows = db_cursor.fetchall()

        return pd.DataFrame(table_rows).to_dict()

    except ConnectionError:
        raise ConnectionError

    finally:
        db_connection.close()


def select_all_cards_from_db():
    """this method created to extract all row from cards.DB
     to create transactions for them.
    Created because Airflow did not allow
    to extract data by means of SQLAlchemy
    """

    db_connection = sql.connect(host=SQL_CR["host"],
                                database=SQL_CR["database"],
                                user=SQL_CR['user'],
                                password=SQL_CR["password"]
                                )
    try:

        logging.warning('select_all_cards_from_db Create connection to DB')

        db_cursor = db_connection.cursor()

        db_cursor.execute(Query['select_card_number'])

        logging.info('Extracting from DB')

        table_rows = db_cursor.fetchall()

        return pd.DataFrame(table_rows).to_dict()

    except ConnectionError:
        raise ConnectionError

    finally:
        db_connection.close()
