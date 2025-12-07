# Databricks notebook source
# MAGIC %run ./0_config

# COMMAND ----------

data_layer = 'bronze'
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

#Reading telemetry data using Spark Streams
df = spark.readStream\
    .format('cloudFiles')\
    .option('cloudFiles.format', 'json')\
    .option('header', 'true')\
    .option('cloudFiles.schemaLocation', f'{raw_path}schema_location/{data_layer}/1') \
    .option('cloudFiles.schemaEvolutionMode', 'rescue')\
    .load(data)

df_writer = df.writeStream\
            .format('delta')\
            .outputMode('append')\
            .option('checkpointLocation', f'{raw_path}checkpoint/{data_layer}/1')\
            .trigger(processingTime=processing_time) \
            .toTable(f'{catalog_name}.{db_name}.sensor_telemetry_data')

df_writer.awaitTermination()