# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,col,sum,year,month,day
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType,LongType

# COMMAND ----------

spark = SparkSession.builder \
    .appName("KafkaJSONConsumer") \
    .getOrCreate()

# COMMAND ----------

# Kafka broker(s) configuration
kafka_broker = "3.18.82.133:9092"  # Replace with your Kafka broker(s)

# COMMAND ----------

# Kafka topic to consume messages from
kafka_topic = "stock_data4"  # Replace with your Kafka topic

# COMMAND ----------

# Define the Kafka consumer configuration
kafka_options = {
    "kafka.bootstrap.servers": kafka_broker,
    "subscribe": kafka_topic,
    "startingOffsets": "earliest"  # Start from the beginning of the topic
}

# COMMAND ----------

# Define the schema for the JSON data
json_schema = StructType([
    StructField("Index", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Open", FloatType(), True),
    StructField("High", FloatType(), True),
    StructField("Low", FloatType(), True),
    StructField("Close", FloatType(), True),
    StructField("Adj Close", FloatType(), True),
    StructField("Volume", FloatType(), True),
    StructField("CloseUSD", FloatType(), True),
])

# COMMAND ----------

# Read JSON messages from Kafka
kafka_stream = spark.read \
    .format("kafka") \
    .options(**kafka_options) \
    .load()

# COMMAND ----------

# Deserialize the JSON value and select it as 'value' column
deserialized_stream = kafka_stream.selectExpr("CAST(value AS STRING) AS json","CAST(key AS STRING) AS key","CAST(offset AS STRING) AS offset","timestamp","timestampType","partition")

# COMMAND ----------

# Parse the JSON data
parsed_stream = deserialized_stream.select('key','timestamp','offset','partition',from_json("json", json_schema).alias("data")).select('key','timestamp','offset','partition',"data.*")

# COMMAND ----------

display(parsed_stream)

# COMMAND ----------

parsed_stream_modified = parsed_stream.withColumn('offset',col("offset").cast("int")) \
                                      .withColumn('year',year(col("Date"))) \
                                      .withColumn('month',month(col("Date"))) \
                                      .withColumn('day',month(col("Date"))) \
                                      .withColumn('AdjClose',col('Adj Close')) \
                                      .select('key','timestamp','offset','partition','Index','Date','Open','High','Low','Close','AdjClose','Volume','CloseUSD','year','month','day')

# COMMAND ----------

parsed_stream_modified.show()

# COMMAND ----------

parsed_stream_modified.count()

# COMMAND ----------

parsed_stream_modified.write.partitionBy('year','month','day').format('delta').mode('append').save('abfss://stockmarketold@stockdata9119.dfs.core.windows.net/')

# COMMAND ----------

df_new = spark.read.format('delta').load('abfss://stockmarketold@stockdata9119.dfs.core.windows.net/')

# COMMAND ----------

df_new

# COMMAND ----------

partitions = df_new.rdd.glom().collect()

for i, partition in enumerate(partitions):
    print(f"Partition {i + 1}: {len(partition)} rows")

# COMMAND ----------

df_new.inputFiles()

# COMMAND ----------


