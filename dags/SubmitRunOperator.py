from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime,timedelta
from airflow.utils.dates import days_ago
 
# Define the default agruments for the DAG
default_args = {
    'owner': 'airflow',                  # Owner of the DAG
    'depends_on_past': False,            # DAG does not depend on past runs
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,                         # Number of retries if the task fails
    'retry_delay': timedelta(minutes=1),  # Delay between retries
}
 
 
 
# Define the DAG  object
dag = DAG(
    'Databrickssubmitrunoperator_v2',       # Name of the DAG
    default_args=default_args,
    description='A DAG to schedule a job daily ',
    schedule_interval= '@daily',                     #  Cron expression to run once daily
    start_date = days_ago(1),           # Start date of the DAG, one day before the current date
    catchup=False
)
 
 
 
# Example of using the JSON parameter to initialize the operator.
new_cluster = {
    "spark_version":  "13.3.x-scala2.12",
    "node_type_id": "Standard_DS3_v2",
    "azure_attributes": {"first_on_demand": 1,"availability": "SPOT_WITH_FALLBACK_AZURE","spot_bid_max_price": -1},
    "num_workers": 1,
}
 
 
 
 
#Parameters for the nootebook task
notebook_task_params = {
    'new_cluster': new_cluster,
    'notebook_task': {
        'notebook_path': '/Users/ranjeet.kumar@cognizant.com/hello_world',
    },
}
 
 
# Creating a DatabricksSubmitRunOperator task
notebook_task = DatabricksSubmitRunOperator(
    task_id='notebook_task',                    # task_ID  
    databricks_conn_id='databricks_conn',       # Connection ID for Databricks
    json=notebook_task_params,
    dag=dag)                                   # Assign the DAG
 
 
 
# Define the task dependencies
notebook_task