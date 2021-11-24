import os
import sys
from datetime import datetime
from datetime import timedelta

from SQLalchemy_task.app import insert

from airflow import DAG
from airflow.operators.python import PythonOperator

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
        'Pandas',
        default_args=default_args,
        description='data extraction',
        schedule_interval=timedelta(minutes=20),
        start_date=datetime(2021, 11, 23),
        catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id='insert_person',
        python_callable=insert,
        dag=dag
    )

    # # t2 = PythonOperator(
    # #     task_id='select_all_person_from_db',
    # #     python_callable=select_all_person_from_db,
    # #
    # #     dag=dag
    # # )
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
    # t1 >> t3
    #
    # # t1 >> t4 >> t5
