from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = (
    SparkSession.builder.appName("KafkaSparkElasticsearch")
    .config("spark.es.nodes", "http://elasticsearch:9200")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

schema = StructType(
    [
        StructField("event_ts_min", StringType(), True),
        StructField("ts_min_bignt", IntegerType(), True),
        StructField("room", StringType(), True),
        StructField("co2", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("light", DoubleType(), True),
        StructField("pir", DoubleType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("movement_status", StringType(), True),
    ]
)

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9093")
    .option("subscribe", "office-input")
    .option("startingOffsets", "earliest")
    .load()
)

df = df.selectExpr("CAST(value AS STRING)")


df = df.withColumn("split_value", split(col("value"), ",")).select(
    col("split_value").getItem(0).alias("event_ts_min"),
    col("split_value").getItem(1).alias("ts_min_bignt").cast(IntegerType()),
    col("split_value").getItem(2).alias("room").cast(StringType()),
    col("split_value").getItem(3).alias("co2").cast(DoubleType()),
    col("split_value").getItem(4).alias("humidity").cast(DoubleType()),
    col("split_value").getItem(5).alias("light").cast(DoubleType()),
    col("split_value").getItem(6).alias("pir").cast(DoubleType()),
    col("split_value").getItem(7).alias("temperature").cast(DoubleType()),
)

df = df.withColumn(
    "movement_status", when(col("pir") > 0.0, "movement").otherwise("no_movement")
)

es_write_conf = {
    "es.nodes": "elasticsearch",
    "es.port": "9200",
    "es.resource": "sensor_data/_doc",
    "es.input.json": "false",
    # "es.mapping.id": "ts_min_bignt",
}

query = (
    df.writeStream.outputMode("append")
    .format("org.elasticsearch.spark.sql")
    .options(**es_write_conf)
    .option("checkpointLocation", "/tmp/checkpoint/dir")
    .start()
)

query.awaitTermination()
