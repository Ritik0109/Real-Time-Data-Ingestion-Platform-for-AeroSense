# Databricks notebook source
class Config:
    def __init__(self):
        self.raw_path = spark.sql('describe external location `landing_zone`').collect()[0].url
        self.data = self.raw_path + "telemetry-data/"
        self.devices = self.raw_path + "devices/"
        self.catalog = f'aerosense_dev'
        self.db_name = 'sensors'
        self.processing_time = '10 seconds'