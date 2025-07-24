# Databricks notebook source
# MAGIC %pip install databricks-dlt
# MAGIC import dlt

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, expr, to_timestamp,when
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType,DateType, ArrayType
from pyspark.sql.functions import to_timestamp

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session with Delta Lake support
spark = SparkSession.builder \
    .appName("DeltaLakeStructuredStreaming") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# COMMAND ----------

#cloudfiles = "dbfs:/FileStore/tmp/csvfileweek3.csv"
cloudfiles = "dbfs:/dbacademy/streaming-delta/source"
checkpoint_dir = "dbfs:/dbacademy/streaming-delta/checkpoints"
Targetpath = "dbfs:/tshaik/newuser/delta/target/stream"

# COMMAND ----------

rawschema = StructType([
    StructField("deviceid", StringType(), True),       # Device as a string
    StructField("heartrate", IntegerType(), True),  # Heartrate as an integer
    StructField("mrn", IntegerType(), True),         # MRN (Medical Record Number) as a string
    StructField("time", TimestampType(), True)      # Time as a timestamp
])

# COMMAND ----------

Readstream = spark.readStream.format("cloudfiles")\
        .option("cloudfiles.format","json")\
        .schema(rawschema)\
        .load("dbfs:/dbacademy/streaming-delta/source")\
        .withColumn("ingestion_time", current_timestamp())

# COMMAND ----------

watermarked_stream = Readstream \
    .withWatermark("time", "10 minutes") \
    .dropDuplicates(["device"])

# COMMAND ----------

from delta.tables import DeltaTable

# Define the Delta table
delta_table_stream = DeltaTable.forPath(spark, "dbfs:/tshaik/newuser/delta/target/stream")

# Define the streaming query
query = watermarked_stream.writeStream \
    .foreachBatch(lambda batch_df, batch_id: 
        delta_table_stream.alias("tgt").merge(
            batch_df.alias("src"),
            "tgt.device = src.device"
        )
        .whenMatchedUpdate(set={"device": "src.device", "heartrate": "src.heartrate", "mrn": "src.mrn"})
        .whenNotMatchedInsert(values={"device": "src.device", "heartrate": "src.heartrate", "mrn": "src.mrn"})
        .execute()
    ) \
    .option("checkpointLocation", "dbfs:/tshaik/newuser/delta/target/checkpoint") \
    .format("delta") \
    .option("mergeSchema",True) \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_dir) \
    .start(Targetpath)

# Start the streaming query
query.awaitTermination()

# COMMAND ----------

delta_table_stream.update(
    condition = "device = ''",
    set = {
           "device": "'47'",
           "mrn": "'65300864'",
           "heartrate": "'100'",
           "time": "'2023-07-22 00:00:00'"
           })

# COMMAND ----------

delta_table_stream.delete("device > '100'")