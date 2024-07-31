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
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['movie', 'movie_db'],
) as dag:

    def branch_fun(ds_nodash):
        import os
        home_dir = os.path.expanduser("~")
        path = os.path.join(home_dir, f"tmp/test_parquet/load_dt={ds_nodash}")
        if os.path.exists(path):
            return rm_dir.task_id
        else:
            return "get.data", "echo.task"

    def get_data(ds_nodash):
        from mov.api.call import save2df
        df = save2df(ds_nodash)
        print(df.head(5))
        
#    def branch_fun(**kwargs):
#	    ld = kwargs['ds_nodash']
#        if os.path.exists(f'~/tmp/test_parquet/load_dt={ld}'):
#            return rm_dir
#        else:
#            return get_data
    def save_data(ds_nodash):
        from mov.api.call import apply_type2df
        
        df = apply_type2df(load_dt=ds_nodash)
        print(df.head(10))
        print(df.dtypes)
        
        # 개봉일 기준 그룹핑 누적 관객수 합
        g = df.groupby('openDt')
        sum_df = g.agg({'audiCnt': 'sum'}).reset_index()
        print(sum_df)


    branch_op = BranchPythonOperator(
	    task_id="branch.op",
    	python_callable=branch_fun
    ) 


    get_data = PythonVirtualenvOperator(
        task_id="get.data",
        python_callable=get_data,
        system_site_packages=False,
        trigger_rule="all_done",
        requirements=["git+https://github.com/Nicou11/movie@0.3/api"],
        venv_cache_path="/home/young12/tmp2/air_venv/get_data"
    )
    
    save_data = PythonVirtualenvOperator(
        task_id="save.data",
        python_callable=save_data,
        system_site_packages=False,
        trigger_rule="one_success",
        requirements=["git+https://github.com/Nicou11/movie@0.3/api"],
        venv_cache_path="/home/young12/tmp2/air_venv/get_data"
    )

    rm_dir = BashOperator(
	    task_id='rm.dir',
	    bash_command='rm -rf ~/tmp/test_parquet/load_dt={{ ds_nodash }}'
    )

    echo_task = BashOperator(
            task_id='echo.task',
            bash_command="echo 'task'"
    )

    end  = EmptyOperator(task_id='end', trigger_rule="all_done")
    start  = EmptyOperator(task_id='start')

    join_task  = BashOperator(
            task_id='join',
            bash_command="exit 1",
            trigger_rule="all_done"
    )
    
    start >> branch_op
    start >> join_task

    branch_op >> rm_dir >> get_data
    branch_op >> echo_task >> save_data
    branch_op >> get_data
    
    join_task >> save_data

    get_data >> save_data >> end



