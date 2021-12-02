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
                f'where c.created_on > {get_previous_date_from_file(file_type="card")}') \
        .option("user", USER["user"]) \
        .option("password", USER["password"]) \
        .option("driver", "com.mysql.jdbc.Driver") \
        .load()
    return df


if __name__ == '__main__':
    dfa = create_df()
    dfa = add_column(dfa)
    previous_d = get_previous_date_from_file(file_type='card')
    max_d = get_max_value_from_df(dfa, file_type='card')
    date_info = create_total(dfa, previous_d, max_d, file_type='card')
    create_part_bank(dfa, previous_d, max_d, file_type='card')
