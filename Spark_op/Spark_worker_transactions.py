from pyspark.sql import SparkSession

from Spark_op.CP.Config.config_file import get_config_URL, get_config_USER
from Spark_op.CP.PathHandler.Path import DB_DRIVER
from Spark_op.spark_oper1.spark_Def import add_column, get_previous_date_from_file, get_max_value_from_df, create_total, \
    create_part_bank

URL = get_config_URL()
USER = get_config_USER()


def create_df():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.driver.extraClassPath", DB_DRIVER) \
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
                f'where transaction_time > {get_previous_date_from_file("transactions")}') \
        .option("user", USER["user"]) \
        .option("password", USER["password"]) \
        .option("driver", "com.mysql.jdbc.Driver") \
        .load()
    return df


if __name__ == '__main__':
    dfa = create_df()
    dfa = add_column(dfa)
    previous_d = get_previous_date_from_file('transactions')
    max_d = get_max_value_from_df(dfa, 'transactions')
    date_info = create_total(dfa, previous_d, max_d, 'transactions')
    create_part_bank(dfa, previous_d, max_d, 'transactions')
