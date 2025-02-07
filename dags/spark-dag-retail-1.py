from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

# Define the DAG
dag = DAG(
    dag_id="retail_analysis_dag_1",
    default_args=default_args,
    description="DAG to run retail analysis using Spark",
    schedule_interval="@daily",
    catchup=False,
)

# Define the SparkSubmitOperator task
retail_analysis = SparkSubmitOperator(
    task_id="retail_analysis",
    application="/spark-scripts/pyspark-retail-1.py",
    conn_id="spark_main",
    conf={
        "spark.jars": "/opt/bitnami/spark/jars/postgresql-42.2.18.jar",
    },
    application_args=[
        "--postgres_host", "dataeng-postgres",
        "--postgres_port", "5433",
        "--postgres_db", "warehouse",
        "--postgres_user", "user",
        "--postgres_password", "*****",
        "--output_path", "/output/retail_analysis.csv",
        "--output_table", "retail_analysis_results",
    ],
    dag=dag,
)


retail_analysis
