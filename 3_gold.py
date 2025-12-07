# Databricks notebook source
# MAGIC %run ./0_config

# COMMAND ----------

data_layer = 'gold'
config = Config()
catalog_name = config.catalog
db_name = config.db_name
raw_path = config.raw_path
devices = config.devices
data = config.data
processing_time = config.processing_time

# COMMAND ----------

#Loading devices data and storing in delta table
df = spark.read\
    .format('json')\
    .option('inferSchema', 'true')\
    .option('header', 'true')\
    .option('multiLine', 'true')\
    .load(devices)

df_writer = df.write\
            .format('delta')\
            .mode('overwrite')\
            .saveAsTable(f'{catalog_name}.{db_name}.devices_mapping_table')

# COMMAND ----------

# Reading Silver data, enriching with new columns and aggregation of data

from pyspark.sql import functions as F

devices_mapping_df = spark.table(
    f"{catalog_name}.{db_name}.devices_mapping_table"
        )

enriched_gold_df = spark.table(f'{catalog_name}.{db_name}.sensor_telemetry_silver')\
                .withWatermark("timestamp", "100 minutes")\
                .groupBy(
                    F.window("timestamp", "1 hour").alias("time_window"),
                    "device_id")\
                .agg(
                    F.avg("temperature").alias("avg_temperature"),
                    F.max("pressure").alias("max_pressure"),
                    F.avg("vibration").alias("avg_vibration"),
                    F.count("*").alias("event_count")
                )

joined_gold_df = enriched_gold_df.join(
    devices_mapping_df,
    on="device_id",
    how='left'
)
                

# COMMAND ----------

#----Write the stream to gold delta table---

gold_stream_writer = joined_gold_df.write\
                    .format("delta")\
                    .option("checkpointLocation",f'{raw_path}checkpoint/{data_layer}/1')\
                    .mode("append")\
                    .saveAsTable(f'{catalog_name}.{db_name}.sensor_telemetry_agg')
