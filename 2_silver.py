# Databricks notebook source
# MAGIC %run ./0_config

# COMMAND ----------

data_layer = 'silver'
config = Config()
catalog_name = config.catalog
db_name = config.db_name
raw_path = config.raw_path
devices = config.devices
data = config.data
processing_time = config.processing_time

# COMMAND ----------

spark.sql(f'create catalog if not exists {catalog_name}')
spark.sql(f'create schema if not exists {catalog_name}.{db_name}')

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

filtered_silver = spark.readStream\
            .table(f'{catalog_name}.{db_name}.sensor_telemetry_data')\
            .dropDuplicates(["device_id","timestamp"])\
            .filter("device_id is not null and timestamp is not null")

updated_silver = filtered_silver\
                .withColumn("battery_level", F.col("battery_level").cast(DoubleType()))\
                .withColumn("ingest_ts", F.to_timestamp("ingest_ts"))\
                .withColumn("timestamp", F.to_timestamp("timestamp"))\
                .withColumn("latitude", F.col("latitude").cast(DoubleType()))\
                .withColumn("longitude", F.col("longitude").cast(DoubleType()))\
                .withColumn("pressure", F.col("pressure").cast(DoubleType()))\
                .withColumn("temperature", F.col("temperature").cast(DoubleType()))\
                .withColumn("vibration", F.col("vibration").cast(DoubleType()))\
                .withColumn("rpm", F.col("rpm").cast(IntegerType())
                )

normalised_silver = updated_silver.filter(
                    (F.col("temperature").between(-50, 150)) &
                    (F.col("pressure") > 0) &
                    (F.col("latitude").between(-90, 90)) &
                    (F.col("longitude").between(-180, 180))
                )

# COMMAND ----------

df_write_to_silver = normalised_silver.writeStream\
                    .format("delta")\
                    .option('checkpointLocation', f'{raw_path}checkpoint/{data_layer}/1')\
                    .option('mergeSchema', 'true')\
                    .outputMode("append")\
                    .trigger(availableNow= True)\
                    .toTable(f'{catalog_name}.{db_name}.sensor_telemetry_silver')