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
)
from mov.api.call import gen_url, req, get_key, req2list, list2df, save2df


with DAG(
    'movie',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='movie_db',
   # schedule_interval=timedelta(days=1),
    schedule="0 4 * * *",
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['movie', 'movie_db'],
) as dag:

    def print_context(ds, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        print("::group::All kwargs")
        pprint(kwargs)
        print(kwargs)
        print("::endgroup::")
        print("::group::Context variable ds")
        print(ds)
        print("::endgroup::")
        return "Whatever you return gets printed in the logs"

    def get_data(ds, **kwargs):
        print(ds)
        print(kwargs)
        print("*" * 20)
        print(f"ds_nodash => {kwargs['ds_nodash']}")
        print(f"kwagrs type => {type(kwargs)}")
        print("*" * 20)
        key = get_key()
        print(f"MOVIE_API_KEY => {key}")
        YYYYMMDD = kwargs['ds_nodash']
        df = save2df(YYYYMMDD)
        print(df.head(5))

    run_this = PythonOperator(
        task_id='print_the_context',
        python_callable=print_context,
    )


    get_data = PythonVirtualenvOperator(
            task_id="get_data",
            python_callable=get_data,
            requirements=["git+http://github.com/Nicou11/movie.git@0.2/api"],
            system_site_packages=False,
    )

    save_data = BashOperator(
        task_id='save.data',
        bash_command="""
            echo "save data"
            """
    )

        #bash_command="""
        #    echo "err report"
        #""",
        #trigger_rule="one_failed"


    end  = EmptyOperator(task_id='end', trigger_rule="all_done")
    start  = EmptyOperator(task_id='start')

    start >> get_data >> save_data >> end
    start >> run_this >> end



