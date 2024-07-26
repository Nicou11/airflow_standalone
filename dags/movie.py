from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    'movie',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='movie_db',
   # schedule_interval=timedelta(days=1),
    schedule="0 4 * * *",
    start_date=datetime(2024, 7, 22),
    catchup=True,
    tags=['movie', 'movie_db'],
) as dag:

    task_get = BashOperator(
        task_id='get.data',
        bash_command="""
            echo "get data" 
    """
    )

    task_save = BashOperator(
        task_id='save.data',
        bash_command="""
            echo "save data"
            """
      #  == awk '{print "{{ds}}, "$2", "$1}' ${CNT_PATH} > ${CSV_PATH}/csv.csv
    )

    task_create_table = BashOperator(
        task_id= "create.table",
        bash_command="""
        """
    )

    task_to_tmp = BashOperator(
        task_id = "to.tmp",
        bash_command="""
        """
    )

    task_to_base = BashOperator(
        task_id="to.base",
        bash_command="""
        """
    )

    task_make_done = BashOperator(
        task_id="make.done",
        bash_command="""
        """
    )

    task_err = BashOperator(
        task_id="err.report",
        bash_command="""
            echo "err report"
        """,
        trigger_rule="one_failed"
    )

    task_end  = EmptyOperator(task_id='end', trigger_rule="all_done")
    task_start  = EmptyOperator(task_id='start')

    task_start >> task_get >> task_save >> task_end



