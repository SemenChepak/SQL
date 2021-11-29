import time
from configparser import ConfigParser

from pyspark.sql import SparkSession, functions, types

config = ConfigParser()
config.read("C:\\Users\\schepak\\SQLALCHEMY_Aitflow\\dags\\SQLalchemy_task\\E_B\\cred\\cred.ini")

URL = f'jdbc:mysql://{config.get("MySQL", "user")}:{config.get("MySQL", "password")}@localhost:' \
      f'{config.get("MySQL", "port")}/{config.get("MySQL", "database")}'

schema = types.StructType([types.StructField('transaction_id', types.IntegerType(), True),
                           types.StructField('card_number', types.StringType(), True),
                           types.StructField('transaction_time', types.IntegerType(), True),
                           types.StructField('comment', types.StringType(), True),
                           types.StructField('value', types.StringType(), True),
                           types.StructField('card_id', types.StringType(), True),
                           types.StructField('holder_id', types.StringType(), True),
                           types.StructField('card_no', types.DateType(), True),
                           types.StructField('valid_until', types.StringType(), True),
                           types.StructField('created_on', types.IntegerType(), True),
                           types.StructField('last_used_on', types.StringType(), True),
                           types.StructField('currency', types.StringType(), True),
                           types.StructField('amount', types.StringType(), True),
                           ])


def create_df():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.driver.extraClassPath", "driver/mysql-connector-java-8.0.27.jar") \
        .getOrCreate()

    df = spark.read \
        .format("jdbc") \
        .option("url", URL) \
        .option('query',
                f'select transaction_id,'
                f' card_number, transaction_time,'
                f' comment, '
                f'value,'
                f' card_id,'
                f' holder_id, '
                f'card_no, '
                f'valid_until, '
                f'created_on, '
                f'last_used_on, '
                f'currency, '
                f'amount from transactions t left join cards c on t.card_number = c.card_no '
                f'where transaction_time > {get_previous_date_from_file()}') \
        .option("user", config.get("MySQL", "user")) \
        .option("password", config.get("MySQL", "password")) \
        .option("driver", "com.mysql.jdbc.Driver") \
        .load()
    return df


def add_column(df):
    return df.withColumn("parquet_created_at", functions.lit(int(time.time())))


def create_bank(df):
    df.write.option("schema", schema).parquet(
        f'output/parquet/transactions/created_between_{get_previous_date_from_file().split(".")[0]}'
        f'_and_{get_max_value_from_df(df).split(".")[0]}')


def get_max_value_from_df(df):
    date = df.agg({"transaction_time": "max"}).collect()[0][0]
    with open('output/max_time/transaction_time_max.txt', 'w') as f:
        f.write(date)
    return date


def get_previous_date_from_file():
    with open('output/max_time/transaction_time_max.txt', 'r') as f:
        d = f.read()
    return d


if __name__ == '__main__':
    dfa = create_df()
    dfa = add_column(dfa)
    create_bank(dfa)
