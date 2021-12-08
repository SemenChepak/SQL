from configparser import ConfigParser

from Spark_op.CP.PathHandler.Path import ROOT_PATH_CREDS_SPARK

config = ConfigParser()
config.read(ROOT_PATH_CREDS_SPARK)


def get_config_URL():
    return f'jdbc:mysql://{config.get("MySQL", "user")}:{config.get("MySQL", "password")}@localhost:{config.get("MySQL", "port")}/{config.get("MySQL", "database")}'


def get_config_USER():
    return {"user": config.get("MySQL", "user"), "password": config.get("MySQL", "password")}

