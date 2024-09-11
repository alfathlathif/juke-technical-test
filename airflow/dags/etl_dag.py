from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_dag',
    default_args=default_args,
    description='ETL process DAG with simple join',
    schedule_interval=timedelta(days=1),
)

# Function to extract data
def extract_data(**kwargs):
    # Simulate extracting data from CSV files stored in VM
    orders_df = pd.read_csv('/path_to_vm/orders.csv')
    customers_df = pd.read_csv('/path_to_vm/customers.csv')
    
    return {'orders_df': orders_df.to_dict(), 'customers_df': customers_df.to_dict()}

# Function to transform data (join operation)
def transform_data(ti, **kwargs):
    # Retrieve data from previous task
    orders_data = ti.xcom_pull(task_ids='extract_data')['orders_df']
    customers_data = ti.xcom_pull(task_ids='extract_data')['customers_df']
    
    orders_df = pd.DataFrame(orders_data)
    customers_df = pd.DataFrame(customers_data)
    
    # Perform join
    transformed_df = pd.merge(orders_df, customers_df, on='CustomerID', how='inner')
    
    # Return transformed data
    return transformed_df.to_dict()

# Function to load data
def load_data(ti, **kwargs):
    # Retrieve transformed data
    transformed_data = ti.xcom_pull(task_ids='transform_data')
    
    # Convert to DataFrame
    transformed_df = pd.DataFrame(transformed_data)
    
    # Save the transformed data to a file in VM
    transformed_df.to_csv('/path_to_vm/transformed_data.csv', index=False)
    print('Data loaded and saved to /path_to_vm/transformed_data.csv')

# Task 1: Extract
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

# Task 2: Transform
transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

# Task 3: Load
load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> load_task
