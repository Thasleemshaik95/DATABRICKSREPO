# Databricks notebook source
# DBTITLE 1,Importing libraries from common utilities
# MAGIC %run "/Workspace/Users/t.police.shaik@accenture.com/Databrick&pypsark tasks/common_utilities"

# COMMAND ----------

# MAGIC %md Dataframe creation notebook
# MAGIC

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Dataframe creation
df = spark.createDataFrame([
	Row(id1=1, Level=4, code='GFG1', date=date(2000, 8, 1),
		datetime=datetime(2000, 8, 1, 12, 0),salary=5000,course=["AWSs", "sql"]),

	Row(id1=2, Level=8, code='GFG2', date=date(2000, 6, 2),
		datetime=datetime(2000, 6, 2, 12, 0),salary=6000,course=["AWS", "PYSPARK"]),

	Row(id1=4, Level=5, code='GFG3', date=date(2000, 5, 3),
		datetime=datetime(2000, 5, 3, 12, 0),salary=7000,course=["AWS", "synapse"]),
 
    Row(id1=1, Level=6, code='GFG3', date=date(2001, 6, 4),
		datetime=datetime(2001, 5, 3, 12, 0),salary=9000,course=["AWS", "datafactory"]),
    
    Row(id1=2, Level=7, code='GFG2', date=date(2002, 6, 2),
		datetime=datetime(2002, 6, 2, 12, 0),salary=8000,course=["AWS", "Databricks"]),
    
    Row(id1=4, Level=8, code='GFG3', date=date(2003, 5, 3),
		datetime=datetime(2003, 5, 3, 12, 0),salary=9000,course=["AWS", "python"]),
    Row(id1=5, Level=12, code='GFG4', date=date(2003, 5, 3),
		datetime=datetime(2003, 5, 3, 12, 0),salary=9000,course=["AWS","Azure"])
    ])
#df.show()
#df.display()

# COMMAND ----------

# DBTITLE 1,explode function
df1=df.select("id1","Level","code","date","datetime","salary",explode(df.course).alias("course"))
#df1.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Appplying tranformations for created Dataframe

# COMMAND ----------


windowsfunc=Window.partitionBy("id1").orderBy(col("date").asc())
df_trans=df1.withColumn("prev_salary",lag("salary",1).over(windowsfunc))\
    .withColumn("total_salary",sum("salary").over(windowsfunc))\
    .withColumn("Avg_salary",avg("salary").over(windowsfunc))\
    .withColumn("Rank",row_number().over(windowsfunc))
#df_trans.show()
#df_trans.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Applying Repartitioning

# COMMAND ----------

dffinalcsv = df_trans
df_trans.repartition(6)
df_partioning = df_trans.repartition("date")
df_partioning_final=df_partioning.repartition(6)
df_final_beforecaching = df_partioning_final.coalesce(1)
df_final = df_final_beforecaching.cache()


# COMMAND ----------

# MAGIC %md
# MAGIC verifying number of partitions in dataframe

# COMMAND ----------

dffinalcsv.rdd.getNumPartitions()

# COMMAND ----------

df_partioning_final.rdd.getNumPartitions()

# COMMAND ----------

df_final.rdd.getNumPartitions()

# COMMAND ----------

# DBTITLE 1,saved to Delta formate
df_final.write.mode("overwrite").format("delta").partitionBy("date").save("dbfs:/tshaik/newuser/tshaikdbf3")

# COMMAND ----------

# DBTITLE 1,saved to csv format
dffinalcsv.write.mode("overwrite").option("header", "true").csv("dbfs:/tshaik/newuser/tshaikdbfcsvnew")

# COMMAND ----------

# DBTITLE 1,reading csv data from dbfs
spark.read.format("csv").option("header", "true").load("dbfs:/tshaik/newuser/tshaikdbfcsvnew").display() #with header
##spark.read.format("csv").load("dbfs:/tshaik/newuser/tshaikdbfcsvnew").display() ---without header

# COMMAND ----------

# DBTITLE 1,reading delta formate data from dbfs
delta_df = spark.read.format("delta").load("dbfs:/tshaik/newuser/tshaikdbf1")
#delta_df.display()
df_trans.show()