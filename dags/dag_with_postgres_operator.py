# We'll start by importing the DAG object
from airflow.decorators import dag, task

# We need to import the operators used in our tasks
from airflow.hooks.postgres_hook import PostgresHook
# We then import the datetime and timedelta function
from datetime import datetime,timedelta

import pandas as pd
import os

# get dag directory path
dag_path = os.getcwd()

# initializing the default arguments that we'll pass to our DAG
default_args = {
    'owner': 'airflow',
    'retries':5,
    'retry_delay':timedelta(minutes=5) 
}

# Establishing connection to PostgreSQL
postgres_hook = PostgresHook('postgres_localhost')
conn = postgres_hook.get_conn()
cursor = conn.cursor()

@dag(
    dag_id = 'booking_ingestion',
    default_args=default_args,
    start_date = datetime(2024,1,2),
    schedule_interval = '@daily',
    catchup=False                         
)

def booking_ingestion():
    @task
    def transform_data():
        booking = pd.read_csv(f"{dag_path}/raw_data/booking.csv", low_memory=False)
        client = pd.read_csv(f"{dag_path}/raw_data/client.csv", low_memory=False)
        hotel = pd.read_csv(f"{dag_path}/raw_data/hotel.csv", low_memory=False)

        # merge booking with client
        data = pd.merge(booking, client, on='client_id')
        data.rename(columns={'name': 'client_name', 'type': 'client_type'}, inplace=True)

        # merge booking, client & hotel
        data = pd.merge(data, hotel, on='hotel_id')
        data.rename(columns={'name': 'hotel_name'}, inplace=True)

        # make date format consistent
        data.booking_date = pd.to_datetime(data.booking_date, infer_datetime_format=True)

        # make all cost in GBP currency
        data.loc[data.currency == 'EUR', ['booking_cost']] = data.booking_cost * 0.8
        data.currency.replace("EUR", "GBP", inplace=True)

        # remove unnecessary columns
        data = data.drop(['address'], axis=1)

        data.to_csv(f"{dag_path}/processed_data/processed_data.csv", index=False)

    @task
    def create_table():
        cursor.execute('''
                    CREATE TABLE IF NOT EXISTS booking_record (
                        client_id int,
                        booking_date date,
                        room_type varchar(250),
                        hotel_id float,
                        booking_cost decimal(10,1),
                        currency varchar(50),
                        age float,
                        client_name varchar(50),
                        client_type varchar(50),
                        hotel_name varchar(50)
                    );
                ''')
        conn.commit()
        cursor.close()
        conn.close()

    @task
    def load_data():
        data = pd.read_csv(f"{dag_path}/processed_data/processed_data.csv")
        # Inserting data into the table
        for index,row in data.iterrows():
            row_dict = row.to_dict()
           # Extract column names and values
            columns = ', '.join(row_dict.keys())
            values = ', '.join(['%s' for _ in row_dict.values()])
            # Create a parameterized query
            query = f"INSERT INTO booking_record ({columns}) VALUES ({values});"
            print('Insert query',query)
            # Execute the query with parameterized values
            cursor.execute(query, tuple(row_dict.values()))
        conn.commit()    
        cursor.close()
        conn.close()

    @task
    def print_success_msg():
        return "Succesfully inserted data to booking_record table"

    transform_data() >> create_table() >> load_data() >> print_success_msg() 

booking_ingestion()