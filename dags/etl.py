from airflow.decorators import dag, task
from datetime import datetime
import json

@dag(
    dag_id = "extract_transform_load",
    start_date = datetime(2024, 1, 1),
    catchup = False,
    schedule_interval = None
)
def sales_analysis():
    '''
    Sales Data Pipeline using the TaskFlow API with 3 tasks - Extract Load Transform
    '''
    @task
    def extract():
        '''
        Reading from the hardcoded JSON string
        '''
        data_string = '{"Year":"2024", "Month":"November","Day":"Thursday","Hour":"12"}'
        data = json.loads(data_string)
        return data
    @task
    def transform(data):
        if "Year" in data and "Month" in data:
            print(f"Month: {data['Month']}, Year: {data['Year']}")
        else:
            print("Year and month not provided")
    @task
    def load(transformed_data):
        return "Loaded"
    data = extract()
    transformed_data = transform(data)
    loaded_data = load(transformed_data)    

dag_instance = sales_analysis()

