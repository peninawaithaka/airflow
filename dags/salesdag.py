from airflow.decorators import dag, task
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator

@dag(
    start_date = datetime(2024, 11, 1),
    schedule = "@hourly",
    dag_id = "penina-dag",
    catchup = False
)
def execute_bash():
    @task()
    def task1():
        BashOperator(
        task_id = 'say_airflow',
        bash_command = 'echo "RUNNING AIRFLOW"'
    )
    @task()
    def task2():
        t2 = EmailOperator(
            task_id = 'send_email',
            to = ['barkotenicholas@gmail.com','peninawaithaka5@gmail.com'],
            subject = 'Airflow Says Hi',
            html_content='<p>This is a test email sent to multiple recipients.</p>'
        )
        t2.execute({})
    task1()
    task2()
execute_bash()    