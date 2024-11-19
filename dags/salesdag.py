from airflow.decorators import dag, task
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator

@dag(
    start_date = datetime(2024, 11, 1),
    schedule = "@daily",
    dag_id = "sales-dag",
    catchup = False
)
def execute_tasks():
    @task()
    def say_hello():
        BashOperator(
        task_id = 'say_airflow',
        bash_command = 'echo "RUNNING AIRFLOW"'
    )
    say_hello()
    @task()
    def send_email():
        email = EmailOperator(
            task_id = 'send_email',
            to = ['barkotenicholas@gmail.com','peninawaithaka5@gmail.com'],
            subject = 'Airflow Says Hi',
            html_content='<p>Sent this email using the sales airflow dag</p>'
        )
        email #execute the email task - email operator is not a callable function
    sending_email_instance = send_email()
execute_tasks()    