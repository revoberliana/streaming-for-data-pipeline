from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Buat SparkSession
spark = SparkSession.builder \
    .appName("KafkaBookStreamingAggregation") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .getOrCreate()

# Konfigurasi Kafka
KAFKA_BROKER = "localhost:9092"  # Sesuaikan dengan Kafka host
KAFKA_TOPIC = "buku_pembelian"  # Sesuaikan dengan Kafka topic

# Definisi Schema JSON sesuai dengan data yang dikirim dari Kafka Producer
schema = StructType([
    StructField("id_book", StringType(), True),
    StructField("author", StringType(), True),
    StructField("title", StringType(), True),
    StructField("genre", StringType(), True),
    StructField("price", IntegerType(), True),
    StructField("stock", IntegerType(), True),
    StructField("publish_id", IntegerType(), True)
])

# Membaca stream dari Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Convert value Kafka dari binary ke string dan parsing JSON
parsed_stream = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Agregasi jumlah total harga buku harian
agg_stream = parsed_stream \
    .groupBy(window(col("publish_id").cast("timestamp"), "1 day")) \
    .agg(spark_sum("price").alias("running_total")) \
    .select(col("window.start").alias("timestamp"), "running_total")

# Menampilkan hasil ke konsol dalam mode complete
query = agg_stream.writeStream \
    .outputMode("complete") \
    .format("console") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()
