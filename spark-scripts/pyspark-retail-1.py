import pyspark
import os
import json
import argparse

from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql.types import StructType
from pyspark.sql.functions import to_timestamp,col,when

postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
postgres_dw_db = os.getenv('POSTGRES_DW_DB')
postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')

# Create Spark session
sparkcontext = pyspark.SparkContext.getOrCreate(conf=(
        pyspark
        .SparkConf()
        .setAppName('Dibimbing')
        .setMaster('local')
        .set("spark.jars", "/opt/postgresql-42.2.18.jar")
    ))
sparkcontext.setLogLevel("WARN")

spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

# Define PostgreSQL connection properties
jdbc_url = f'jdbc:postgresql://{postgres_host}/{postgres_dw_db}'
jdbc_properties = {
    'user': postgres_user,
    'password': postgres_password,
    'driver': 'org.postgresql.Driver',
    'stringtype': 'unspecified'
}

# Load data from PostgreSQL
print("Reading data from PostgreSQL 'retail' table...")
retail_df = spark.read.jdbc(
    url="jdbc:postgresql://your_host:your_port/your_db",
    table="retail",
    properties={"user": "your_user", "password": "your_password", "driver": "org.postgresql.Driver"}
)
