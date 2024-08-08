from datetime import datetime, timedelta
from textwrap import dedent
#from pprint import pprint as pp

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

    REQUIREMENTS=["git+https://github.com/Nicou11/mov_agg@0.5/agg"]

    def gen_empty(*ids):
        tasks = []
        for id in ids:
            task = EmptyOperator(task_id=id)
            tasks.append(task)
        return tuple(tasks)


    def gen_vpython(**kw):
        task = PythonVirtualenvOperator(
            task_id=kw['id'],
            python_callable=kw['fun_obj'],
            system_site_packages=False,
            requirements=REQUIREMENTS,
            op_kwargs=kw['op_kw'],
            )
        return task
    

    def pro_data(**params):
        print(params['task_name'])
        print(params) # task_name
        print("@"*33)

    def pro_merge(task_name, **params):
        load_dt = params['ds_nodash']
        from mov_agg.util import merge
        df = merge(load_dt)
        print("*"*33)
        print(df)

    def pro_data3(task_name):
        print(task_name)
        print("@"*33)

    def pro_data4(task_name, ds_nodash, **kwargs):
        print(task_name)
        print(ds_nodash)
        print(kwargs)
        print("@"*33)

    def delete():
        print(delete)


    def summary():
        print(summary)


    #start  = gen_empty(id='start')
    #end  = gen_empty('end', trigger_rule="all_done")
    start, end = gen_empty('start', 'end')
    
    apply_type = gen_vpython(
            id = 'apply.type',
            fun_obj = pro_data,
            op_kw = {"task_name": "apply_type!!!"}
            )

    merge = gen_vpython(
            id = 'merge.df',
            fun_obj = pro_merge,
            op_kw = 'load_dt'
            )
    
    del_du = gen_vpython(
            id = 'de.dup',
            fun_obj = pro_data3,
            op_kw = {"task_name": "del_du!!!"}
            )

    summary_df = gen_vpython(
            id = "summary.df",
            fun_obj = pro_data4,
            op_kw = { "task_name": "summary_df!!!" }
            )


    start >> merge
    merge >> del_du >> apply_type
    apply_type >> summary_df >> end
    

