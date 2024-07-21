from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    'traffic',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': True,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='traffic DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['traffic'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    task_Weekday_Mor = BashOperator(
        task_id='Weekday_Morning',
        bash_command='date',
    )

    task_Weekday_After = BashOperator(
        task_id='Weekday_Afternoon',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3,
    )

    task_Weekend = DummyOperator(task_id='Weekend')
    task_Weekday = DummyOperator(task_id='Weekday')
    task_Weekday_Section = DummyOperator(task_id='Weekday_Section')
    task_Weekend_Section = DummyOperator(task_id='Weekend_Section')
    task_Weekend_Mor = DummyOperator(task_id='Weekend_Morning')
    task_Weekend_After = DummyOperator(task_id='Weekend_Afternoon')
    task_Most_free_time  = DummyOperator(task_id='Most_free_time')
    task_Capital_Region_First_Ring_Expressway_traffic = DummyOperator(task_id="Capital_Region_First_Ring_Expressway_traffic")

    task_Capital_Region_First_Ring_Expressway_traffic >> [task_Weekday, task_Weekend]
    task_Weekday >> task_Weekday_Section >> [task_Weekday_Mor, task_Weekday_After] >> task_Most_free_time
    task_Weekend >> task_Weekend_Section >> [task_Weekend_Mor, task_Weekend_After] >> task_Most_free_time 
