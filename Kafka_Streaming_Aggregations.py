# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

from pyspark.sql.types import DateType,TimestampType,IntegerType

# COMMAND ----------

# Set up a Spark session
spark = SparkSession.builder \
    .appName("KafkaJSONConsumer") \
    .getOrCreate()

# COMMAND ----------

# Kafka broker(s) configuration
kafka_broker = "3.18.82.133:9092"  # Replace with your Kafka broker(s)

# COMMAND ----------

# Kafka topic to consume messages from
kafka_topic = "stock_data2"  # Replace with your Kafka topic

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
    StructField("CreatedTime", StringType(), True),
    StructField("Type", StringType(), True),
    StructField("Amount", IntegerType(), True),
    StructField("BrokerCode", StringType(), True),
])

# COMMAND ----------

# Read JSON messages from Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .options(**kafka_options) \
    .load()


# COMMAND ----------

# Deserialize the JSON value and select it as 'value' column
deserialized_stream = kafka_stream.selectExpr("CAST(value AS STRING) AS json","CAST(key AS STRING) AS key","CAST(offset AS STRING) AS offset","timestamp","timestampType")

# COMMAND ----------

# MAGIC %md
# MAGIC display(deserialized_stream)

# COMMAND ----------

# Parse the JSON data
parsed_stream = deserialized_stream.select(from_json("json", json_schema).alias("data")).select("data.*")

# COMMAND ----------

# MAGIC %md
# MAGIC parsed_stream.show()

# COMMAND ----------

# MAGIC %md
# MAGIC parsed_stream.show()

# COMMAND ----------

# Display the streaming DataFrame (for demonstration)
query = parsed_stream \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/delta/events8/_checkpoints/") \
    .table("stock_data.stock1.results") 




# COMMAND ----------

from pyspark.sql.functions import col,when,to_timestamp

# COMMAND ----------

parsed_pivot_stream = parsed_stream.withColumn("BUY", when(parsed_stream["Type"] == 'BUY', parsed_stream["Amount"]).otherwise(0)) \
                                   .withColumn("SELL", when(parsed_stream["Type"] == 'SELL', parsed_stream["Amount"]).otherwise(0)) \
                                   .withColumn('Created_time',to_timestamp(parsed_stream['CreatedTime']))
                                   

# COMMAND ----------

parsed_pivot_stream = parsed_pivot_stream.select("Created_time","BUY","SELL","BrokerCode")

# COMMAND ----------

# MAGIC %md
# MAGIC parsed_pivot_stream.show()

# COMMAND ----------

from pyspark.sql.functions import window,window_time,sum

# COMMAND ----------

aggregated_stream = parsed_pivot_stream.withWatermark("Created_time", "10 minute") \
                        .groupBy(window(col('Created_time'),"2 minute")) \
                    .agg(sum("BUY").alias ('Total_buy'),sum("SELL").alias("Total_sell"))

# COMMAND ----------

# MAGIC %md
# MAGIC display(aggregated_stream)

# COMMAND ----------

output_df = aggregated_stream.select(aggregated_stream['window'].start.alias('window_start'),aggregated_stream['window'].end.alias('window_end'),'Total_buy','Total_sell')

# COMMAND ----------

# MAGIC %md
# MAGIC display(output_df)

# COMMAND ----------

output_df \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/delta/events5/_checkpoints/") \
    .table("stock_data.stock1.totals") 

# COMMAND ----------



# COMMAND ----------

query.awaitTermination()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, TimestampType
from datetime import datetime, timedelta

# Create a Spark session
spark = SparkSession.builder.appName("HourlyKafkaConsumer").getOrCreate()

# Define the schema for the Kafka messages
schema = StructType().add("key", StringType()).add("value", StringType()).add("timestamp", TimestampType())

# Calculate the start timestamp for the current hour
current_hour_start = datetime.now().replace(minute=0, second=0, microsecond=0)

# Read from Kafka topic in batch mode with a starting offset for the current hour
kafka_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "your_topic") \
    .option("startingOffsets", f"timestamp={int(current_hour_start.timestamp()) * 1000}") \
    .load() \
    .selectExpr("CAST(value AS STRING") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

# Show the data
kafka_df.show()

# You can now process or write the data as needed


# COMMAND ----------

from datetime import datetime, timedelta

# COMMAND ----------

datetime.now()

# COMMAND ----------

current_hour_start = datetime.now().replace(minute=0, second=0, microsecond=0)

# COMMAND ----------

int(current_hour_start.timestamp()) * 1000

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, TimestampType
from datetime import datetime

# Create a Spark session
spark = SparkSession.builder.appName("HourlyKafkaConsumer").getOrCreate()

# Define the schema for the Kafka messages
schema = StructType().add("key", StringType()).add("value", StringType()).add("timestamp", TimestampType())

# Get the current hour as a string (e.g., "2023-01-01-15" for the 15th hour of January 1, 2023)
current_hour_str = datetime.now().strftime("%Y-%m-%d-%H")

# Read from Kafka topic in batch mode using subscribePattern
kafka_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribePattern", f"your_topic_{current_hour_str}") \
    .load() \
    .selectExpr("CAST(value AS STRING") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

# Show the data
kafka_df.show()

# You can now process or write the data as needed


# COMMAND ----------

current_hour_str = datetime.now().strftime("%Y-%m-%d-%H")

# COMMAND ----------

current_hour_str

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, TimestampType
from datetime import datetime, timedelta

# Create a Spark session
spark = SparkSession.builder.appName("TimeWindowKafkaConsumer").getOrCreate()

# Define the schema for the Kafka messages
schema = StructType().add("key", StringType()).add("value", StringType()).add("timestamp", TimestampType())

# Specify the time window (adjust as needed)
start_time = datetime.now() - timedelta(hours=2)  # Example: 2 hours ago
end_time = datetime.now()  # Current time

# Convert the time window to strings
start_time_str = start_time.strftime("%Y-%m-%d-%H")
end_time_str = end_time.strftime("%Y-%m-%d-%H")

# Generate a pattern for the time window
time_window_pattern = f"your_topic_{{({start_time_str}..{end_time_str})}}"

# Read from Kafka topic in batch mode using subscribePattern
kafka_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribePattern", time_window_pattern) \
    .load() \
    .selectExpr("CAST(value AS STRING") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

# Show the data
kafka_df.show()

# You can now process or write the data as needed

