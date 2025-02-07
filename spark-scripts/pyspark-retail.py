from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# Define default_args for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 30),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'retail_analysis_dag',
    default_args=default_args,
    description='Retail Analysis with Spark',
    schedule_interval=None,  # Change this to a cron expression if you want it to run on a schedule
    catchup=False,
)

# Define the SparkSubmitOperator
spark_submit_task = SparkSubmitOperator(
    task_id='retail_analysis',
    application='/spark-scripts/pyspark-retail.py',  # Path to your Python script
    conn_id='spark_main',  # Airflow connection to Spark (ensure this is set up)
    conf={
        "spark.master": "spark://dataeng-spark-master:7077",
        "spark.jars": "/opt/bitnami/spark/jars/postgresql-42.2.18.jar",
    },
    name='retail-analysis',
    application_args=[
        '--postgres_host', 'dataeng-postgres',
        '--postgres_port', '5433',
        '--postgres_db', 'warehouse',
        '--postgres_user', 'user',
        '--postgres_password', '<password>',
    ],
    dag=dag,
)

# Set up dependencies (if any)
spark_submit_task
