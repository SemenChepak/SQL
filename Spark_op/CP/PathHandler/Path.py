ROOT_PATH_CREDS = '/cred/cred.ini'
DB_DRIVER = "driver/mysql-connector-java-8.0.27.jar"
CARD_DATE_HANDLER = 'output/max_time/card_max_created_at.txt'
TRANSACTION_DATE_HANDLER = 'output/max_time/transaction_time_max.txt'
ROOT_PATH_CREDS_SPARK = 'C:\\Users\\schepak\\SQLALCHEMY_Aitflow\\dags\\packages\\E_B\\cred\\cred.ini'
DATE_HANDLER = {'card': 'output/max_time/card_max_created_at.txt',
                'transactions': 'output/max_time/transaction_time_max.txt'}
OUTPUT_HANDLER = {'card': 'file:///C:/Users/schepak/SQLALCHEMY_Aitflow/Spark_op/output/parquet/person_cards',
                  'transactions': 'file:///C:/Users/schepak/SQLALCHEMY_Aitflow/Spark_op/output/parquet/transactions'}
