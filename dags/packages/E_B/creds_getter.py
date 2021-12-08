from configparser import ConfigParser
from packages.E_B.path_holder.path import CRED

config = ConfigParser()
config.read(CRED)


def get_card_table_creds():
    return {'card_lifetime_seconds': config.get("card_table", "card_lifetime_seconds")}


def get_transactions_table_creds():
    return {'transaction_range_min': config.get("transaction_table", "transaction_range_min"),
            'transaction_range_max': config.get('transaction_table', 'transaction_range_max')}


def get_creds_sql():
    return {'database': config.get("MySQL", "database"),
            'user': config.get("MySQL", "user"),
            'password': config.get("MySQL", "password"),
            'host': config.get("MySQL", "host"),
            'port': config.get("MySQL", "port"),
            }


def create_eng():
    return config.get("MySQL", "engine")

def get_sql():
    return {'select_customer_id': config.get("SQL_Query", "select_customer_id"),
            'select_card_number': config.get("SQL_Query", "select_card_number")}


def log_path():
    return config.get("log", "path")
