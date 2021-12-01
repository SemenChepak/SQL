import time
from configparser import ConfigParser

from pyspark.sql import SparkSession, functions, types

from path.Path import ROOT_PATH_SPARK, ROOT_PATH_WINDOWS

config = ConfigParser()
config.read(f"{ROOT_PATH_WINDOWS}\\SQLALCHEMY_Aitflow\\dags\\SQLalchemy_task\\E_B\\cred\\cred.ini")

URL = f'jdbc:mysql://{config.get("MySQL", "user")}:{config.get("MySQL", "password")}@localhost:' \
      f'{config.get("MySQL", "port")}/{config.get("MySQL", "database")}'

schema = types.StructType([types.StructField('holder_id', types.IntegerType(), True),
                           types.StructField('holder_id_code', types.StringType(), True),
                           types.StructField('holder_name', types.IntegerType(), True),
                           types.StructField('holder_second_name', types.StringType(), True),
                           types.StructField('holder_surname', types.StringType(), True),
                           types.StructField('phone', types.StringType(), True),
                           types.StructField('city', types.StringType(), True),
                           types.StructField('holder_birth_date', types.DateType(), True),
                           types.StructField('card_id', types.StringType(), True),
                           types.StructField('card_no', types.IntegerType(), True),
                           types.StructField('valid_until', types.StringType(), True),
                           types.StructField('created_on', types.StringType(), True),
                           types.StructField('last_used_on', types.StringType(), True),
                           types.StructField('currency', types.StringType(), True),
                           types.StructField('amount', types.StringType(), True),
                           types.StructField('parquet_created_at', types.StringType(), True),
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
                f'select id_code,'
                f' customer_id, name,'
                f' second_name, '
                f'surname,'
                f' phone,'
                f' city, '
                f'birth_date, '
                f'card_id, '
                f'holder_id, '
                f'card_no, '
                f'valid_until, '
                f'created_on, '
                f'last_used_on, '
                f'currency, '
                f'amount from people p left join cards c on p.customer_id = c.holder_id '
                f'where c.created_on > {get_previous_date_from_file()}') \
        .option("user", config.get("MySQL", "user")) \
        .option("password", config.get("MySQL", "password")) \
        .option("driver", "com.mysql.jdbc.Driver") \
        .load()
    return df


def add_column(df):
    return df.withColumn("parquet_created_at", functions.lit(int(time.time())))


def create_total(df, previous_date, max_date):
    df.write.option("schema", schema).parquet(
        f'file:///{ROOT_PATH_SPARK}/SQLALCHEMY_Aitflow/dags/SQLalchemy_task/Spark_op/output/parquet/person_cards/created_between_{previous_date}_and_{max_date}/total')
    return {'previous_date': previous_date, 'max_date': max_date}


def get_max_value_from_df(df):
    date = df.agg({"created_on": "max"}).collect()[0][0]
    with open('output/max_time/card_max_created_at.txt', 'w') as f:
        f.write(date)
    return date.split(".")[0]


def get_previous_date_from_file():
    with open('output/max_time/card_max_created_at.txt', 'r') as f:
        d = f.read()
    return d.split(".")[0]


def create_part_bank(df, previous_date, max_date):
    distinct_val = df.select('created_on').distinct().collect()
    for i in distinct_val:
        df.filter(df['created_on'] == i[0]).collect()
        df.write.option("schema", schema).parquet(
            f'file:///{ROOT_PATH_SPARK}/SQLALCHEMY_Aitflow/dags/SQLalchemy_task/Spark_op/output/parquet/person_cards/created_between_{previous_date}'
            f'_and_{max_date}/bank/created_on_{i[0]}')


if __name__ == '__main__':
    dfa = create_df()
    dfa = add_column(dfa)
    previous_d = get_previous_date_from_file()
    max_d = get_max_value_from_df(dfa)
    date_info = create_total(dfa, previous_d, max_d)
    create_part_bank(dfa, previous_d, max_d)
