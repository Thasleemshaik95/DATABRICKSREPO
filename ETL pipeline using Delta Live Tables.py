# Databricks notebook source
# MAGIC %pip install databricks-dlt
# MAGIC import dlt

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/tshaik/newuser/delta/delta")

# COMMAND ----------

#dbutils.fs.mkdirs("dbfs:/tshaik/newuser/delta/raw")   ##raw 
#dbutils.fs.mkdirs("dbfs:/tshaik/newuser/delta/target/stream")
#dbutils.fs.mkdirs("dbfs:/tshaik/newuser/delta/raw/target")    ###final target

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, from_json, expr
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType, ArrayType

# COMMAND ----------

raw_schema = StructType([
    StructField("image_id", StringType(), True),
    StructField("study_id", StringType(), True),
    StructField("modality", StringType(), True),
    StructField("acquisition_time", TimestampType(), True),
    StructField("resolution", StringType(), True),
    StructField("patient_id", StringType(), True),
    StructField("tags", ArrayType(StringType()), True)
])

# COMMAND ----------

cloudFiles = "dbfs:/FileStore/tmp/filesjson.txt"  ##source
stream_cloudFiles = "dbfs:/dbacademy/streaming-delta/source/01.json"

# COMMAND ----------

#dlt.enable_local_execution(); ##dlt.enable_local_execution()
@dlt.table(
    name="bronze_anir_metadata",
    comment="Raw ANIR image metadata from source."
)
def load_bronze(): # Comment out DLT decorator for local testing
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format","json")
        .schema(raw_schema)
        .load("/tshaik/newuser/delta/raw")
        .withColumn("ingestion_time", current_timestamp())
    )

# COMMAND ----------

# DBTITLE 1,a
#dlt.enable_local_execution(); ##dlt.enable_local_execution()
@dlt.table(
    name="silver_anir_metadata",
    comment="Validated ANIR image metadata with schema enforcement."
)
@dlt.expect("valid_modality", "modality IN ('MRI', 'CT', 'XRay')")
@dlt.expect_or_drop("non_null_patient", "patient_id IS NOT NULL")
#@dlt.expect_or_fail("image_resolution_check", "resolution RLIKE '^[0-9]+x[0-9]+$'")
def clean_silver():
    df_clean_silver = dlt.read("bronze_anir_metadata")

    df_clean_silver_final = df_clean_silver.select(
        col("image_id"),
        col("study_id"),
        col("modality"),
        col("acquisition_time"),
        col("resolution"),
        col("patient_id"),
        col("tags"),
        col("ingestion_time")
    )
    return df_clean_silver_final

# COMMAND ----------

#dlt.enable_local_execution(); ##dlt.enable_local_execution()
@dlt.table(
    name="gold_patient_image_summary",
    comment="Summary of images per patient for ANIR analysis."
)
def gold_summary():
    df_summary = dlt.read("silver_anir_metadata")
    #df_summary = spark.sql("SELECT * FROM silver_anir_metadata")
    df_final = df_summary.groupBy("patient_id").agg(
            expr("count(*) as total_images"),
            expr("count(distinct modality) as unique_modalities"),
            expr("max(acquisition_time) as last_scan_time")
            )
    return df_final
    #display(df_final)

# COMMAND ----------

#df_final.write.format("delta").mode("overwrite").save("dbfs:/tshaik/newuser/delta/raw/target")