from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
#from airflow.models import Variable

def gen_emp(id, rule="all_success"):
    op = EmptyOperator(task_id=id, trigger_rule=rule)
    return op

with DAG(
    'make_parquet',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='Parquet DAG',
    schedule="0 4 * * *",
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['make_parquet'],
) as dag:

    task_check_done = BashOperator(
        task_id='check.done',
        bash_command="""
            DONE_FILE={{ var.value.IMPORT_DONE_PATH }}/{{ds_nodash}}/_DONE
            bash {{ var.value.CHECK_SH }} $DONE_FILE
        """
    )

    task_to_parquet = BashOperator(
        task_id='to.parquet',
        bash_command="""
            echo "to.parquet"

            READ_PATH="~/data/csv/{{ds_nodash}}/csv.csv"
            SAVE_PATH="~/data/parquet/"

            mkdir -p $SAVE_PATH
            python ~/airflow/py/csv2parquet.py $READ_PATH $SAVE_PATH
            
            
        """
    )
    
    task_done = BashOperator(
        task_id='make.done',
        bash_command="""

        """,
    )

    task_err = BashOperator(
        task_id='err.report',
        bash_command="""
            echo "err report"
        """,
        trigger_rule="one_failed"
    )

    task_end  = gen_emp('end', 'all_done') 
    task_start  = gen_emp('start')

    task_start >> task_check_done >> task_to_parquet
    task_check_done >> task_err >> task_end
    task_to_parquet >> task_done >> task_end
