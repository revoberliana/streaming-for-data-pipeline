from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

dag = DAG(
    dag_id="retail_analysis_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)

# Define the SparkSubmitOperator for running the PySpark job
retail_analysis = SparkSubmitOperator(
    task_id="retail_analysis",  # Task ID in Airflow
    application="/spark-scripts/pyspark-retail.py",  # Path to your Spark script
    conn_id="spark_main",  # Connection ID for Spark (ensure this is correctly set up in Airflow UI)
    conf={"spark.master": "spark://dataeng-spark-master:7077"},  # Correct Spark master configuration (no need for YARN)
    application_args=[  # Arguments to pass to the Spark application
        "--postgres_host", "dataeng-postgres",
        "--postgres_port", "5433",
        "--postgres_db", "warehouse",
        "--postgres_user", "user",
        "--postgres_password", "password"
    ],
    dag=dag,  # Add this task to the DAG
)

