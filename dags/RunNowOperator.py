from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
 
 
# Define the default agruments for the DAG
default_args = {
    'owner': 'airflow',                    # Owner of the DAG
    'depends_on_past': False,              # DAG does not depend on past runs
    'email_on_failure': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,                          # Number of retries if the task fails
    'retry_delay': timedelta(minutes=1),   # Delay between retries
}
 
 

# Define the DAG  object
dag = DAG(
    'DatabricksRunNowOperator_V1',                        # Name of the DAG
    default_args=default_args,
    description='A simple DAG to run Databricks jobs',
    schedule_interval='@daily',                            # Cron expression to run once  daily
    start_date=days_ago(1),                                # Start date of the DAG, one day before the current date
    tags=['example'],
)
 
 
 
 
# Creating a DatabricksRunNowOperator task
run_notebook1 = DatabricksRunNowOperator(
    task_id='run_notebook1',                              # task_ID  
    dag=dag,                                              # Assign the DAG
    databricks_conn_id='databricks_conn',                 #Connection ID for Databricks
    job_id = 249904516309392,                             #   Job ID for the Databricks job to be submitted
 
)
 
 
run_notebook2 = DatabricksRunNowOperator(
        task_id='run_notebook2',                     # task_ID
        dag = dag,                                   # Assign the DAG
        databricks_conn_id='databricks_conn',        # Connection ID for Databricks
        job_id = 597792715970891,                    # Job ID for the Databricks job to be submitted
)
 
 
 
# Define the task dependencies
run_notebook1 >> run_notebook2        # Set run_notebook1  to precede run_notebook2
 
 
 