import os
import sys
from datetime import datetime
from datetime import timedelta

from SQLalchemy_task.app import insert, check_db, create_all
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

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
        'Alchemy',
        default_args=default_args,
        description='data extraction',
        schedule_interval=timedelta(minutes=20),
        start_date=datetime(2021, 11, 23),
        catchup=False,
) as dag:
    check_db_exist = PythonOperator(
        task_id='check_db_exist',
        python_callable=check_db,
        retries=1,
        dag=dag)

    check_db_exist2 = PythonOperator(
        task_id='check_db_exist2',
        python_callable=check_db,
        retries=1,
        dag=dag
    )
    insert_into_db = PythonOperator(
        task_id='insert_into_db',
        python_callable=insert,
        trigger_rule=TriggerRule.ONE_SUCCESS,
        dag=dag
    )
    insert_into_db2 = PythonOperator(
        task_id='insert_into_db2',
        python_callable=insert,
        trigger_rule=TriggerRule.ONE_SUCCESS,
        dag=dag
    )
    create_all_tables = PythonOperator(
        task_id='create_all_tables',
        python_callable=create_all,
        trigger_rule=TriggerRule.ONE_FAILED,
        dag=dag
    )
    # t3 = PythonOperator(
    #     task_id='insert_cards',
    #     python_callable=insert_cards,
    #     op_args=t1.output,
    #     dag=dag
    # )
    #
    # t4 = PythonOperator(
    #     task_id='select_all_card_from_db',
    #     python_callable=select_all_card_from_db,
    #
    #     dag=dag
    # )
    #
    # t5 = PythonOperator(
    #     task_id='insert_transactions',
    #     python_callable=insert_transactions,
    #     op_args=t4.output,
    #     dag=dag
    # )
    #
    check_db_exist >> insert_into_db
    check_db_exist >> create_all_tables >> check_db_exist2 >> insert_into_db2
    check_db_exist2 >> insert_into_db2