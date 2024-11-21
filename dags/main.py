import textwrap
from datetime import datetime, timedelta


from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

#instantiating the dag - add default arguments
with DAG (
    'learning', #name of the dag
    default_args={
        "depends_on_past":False,
        "email_on_failure":True,
        "email":['peninawaithaka5@gmail.com'],
        "retries":1,
        "retry_delay":timedelta(minutes=5)
},
    description = "Airflow Project",
    schedule = timedelta(days=1),
    start_date = datetime(2024, 1, 1),
    catchup=False
) as dag:
    
    t1 = BashOperator(
        task_id = 'print_date',
        bash_command = "date",
    )
    t2 = BashOperator(
        task_id = 'sleep',
        depends_on_past = False,
        bash_command = "sleep 5",
        retries = 3 #overwrites the default retries set
    )
    t1 >> t2 #dependencies set up




