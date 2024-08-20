from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    PythonOperator,
    PythonVirtualenvOperator,
    BranchPythonOperator
)

with DAG(
    'movie_dynamic_json',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='movie_dynamic_json',
    schedule="@once",
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2015, 1, 2),
    catchup=True,
    tags=['movie', 'dynamic', 'json'],
) as dag:

    def get_data(ds_nodash):
        from movdata.movielist import save_movie_json
        year = str(ds_nodash)[:4]
        file_path = "/home/young12/data/json/movie.json"

        save_movie_json(year, file_path)  
        return True

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end', trigger_rule="all_done")

    get_data = PythonVirtualenvOperator(
        task_id='get.data',
        python_callable=get_data,
        requirements=["git+https://github.com/Nicou11/movdata.git@0.2/movielist"],
        system_site_packages=False,
    )

    pars_parq = BashOperator(
        task_id='parsing.parquet',
        bash_command="""
            $SPARK_HOME/bin/spark-submit /home/young12/airflow/py/parsing_parquet.py {{execution_date.year}}
        """
    )

    sel_parq = BashOperator(
        task_id='select.parquet',
        bash_command="""
            $SPARK_HOME/bin/spark-submit /home/young12/airflow/py/select_parquet.py {{execution_date.year}}
        """
    )

    start >> get_data >> pars_parq >> sel_parq >> end
