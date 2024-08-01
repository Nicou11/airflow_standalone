from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    ExternalPythonOperator,
    PythonOperator,
    PythonVirtualenvOperator,
    is_venv_installed,
    BranchPythonOperator
)
import os

with DAG(
    'movie_summary',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    max_active_runs=1,
    max_active_tasks=3,
    description='movie_summary',
   # schedule_interval=timedelta(days=1),
    schedule="10 2 * * *",
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['movie', 'movie_summary'],
) as dag:
    
    def apply():
        print(apply)


    def merge():
        print(merge)

    
    def delete():
        print(delete)


    def summary():
        print(summary)


    apply = PythonVirtualenvOperator(
        task_id='apply_type.task',
        python_callable=apply,
        system_site_packages=False,
    )


    merge = PythonVirtualenvOperator(
        task_id='merge.task',
        python_callable=merge,
        system_site_packages=False,
    )


    del_du = PythonVirtualenvOperator(
        task_id='del_du.task',
        python_callable=delete,
        system_site_packages=False,
    )


    summary = PythonVirtualenvOperator(
        task_id='summary_df.task',
        python_callable=summary,
        system_site_packages=False,
    )


    end  = EmptyOperator(task_id='end', trigger_rule="all_done")
    start  = EmptyOperator(task_id='start')

    start >> apply >> merge >> del_du >> summary >> end
    

