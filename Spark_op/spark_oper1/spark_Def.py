import time

from pyspark.sql import functions

from Spark_op.CP.Config.config_file import get_config_URL, get_config_USER
from Spark_op.CP.PathHandler.Path import DATE_HANDLER, OUTPUT_HANDLER
from Spark_op.CP.SH.Shema import SCHEMA

URL = get_config_URL()
USER = get_config_USER()


def add_column(df):
    return df.withColumn("parquet_created_at", functions.lit(int(time.time())))


def get_previous_date_from_file(file_type):
    with open(DATE_HANDLER[file_type], 'r') as f:
        d = f.read()
    return d.split(".")[0]


def get_max_value_from_df(df, file_type):
    date = df.agg({"created_on": "max"}).collect()[0][0]
    with open(DATE_HANDLER[file_type], 'w') as f:
        f.write(date)
    return date.split(".")[0]


def create_total(df, previous_date, max_date, file_type):
    df.write.option("schema", SCHEMA[file_type]). \
        parquet(
        f'{OUTPUT_HANDLER[file_type]}/'
        f'created_between_{previous_date}_and_{max_date}/total'
        )
    return {'previous_date': previous_date, 'max_date': max_date}


def create_part_bank(df, previous_date, max_date, file_type):
    distinct_val = df.select('created_on').distinct().collect()
    for i in distinct_val:
        df.filter(df['created_on'] == i[0]).collect()
        df.write.option("schema", SCHEMA[file_type]). \
            parquet(
            f'{OUTPUT_HANDLER[file_type]}/'
            f'created_between_{previous_date}_and_{max_date}'
            f'/bank/created_on_{i[0]}'
            )
