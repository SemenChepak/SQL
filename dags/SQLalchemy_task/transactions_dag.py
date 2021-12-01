import os
import sys
from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from SQLalchemy_task.app import check_db, create_all, insert_tr

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

default_args = {
    'owner': 'Alchemy',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=20),
}

with DAG(
        'Alchemy_transactions',
        default_args=default_args,
        description='data extraction',
        schedule_interval=timedelta(minutes=5),
        start_date=datetime(2021, 11, 23),
        catchup=False,
) as dag:
    check_db_exist = PythonOperator(
        task_id='check_db_exist',
        python_callable=check_db,
        retries=1,
        dag=dag)

    create_all_tables = PythonOperator(
        task_id='create_all_tables',
        python_callable=create_all,
        trigger_rule=TriggerRule.ONE_FAILED,
        dag=dag
    )


    insert_tr = PythonOperator(
        task_id='insert_tr',
        python_callable=insert_tr,
        trigger_rule=TriggerRule.ONE_SUCCESS,
        dag=dag
    )

    check_db_exist >>  insert_tr
    check_db_exist >> create_all_tables
