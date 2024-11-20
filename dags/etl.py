from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id = "extract_transform_load",
    start_date = datetime(2024, 1, 1),
    catchup = False,
    schedule_interval = None
)
def sales_analysis():
    @task
    def extract():
        return "Extracted"
    @task
    def transform():
        return "Transformed"
    @task
    def load():
        return "Loaded"
    extract()
    transform()
    load()

dag_instance = sales_analysis()

