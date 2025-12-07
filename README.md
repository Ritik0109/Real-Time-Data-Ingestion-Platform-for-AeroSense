# Real-Time-Data-Ingestion-Platform-for-AeroSense

### About
Aerosphere is a leader in industrial IoT solutions, providing predictive maintenance and operational insights for heavy machinery. Their devices generate high-volume, real-time telemetry data (sensor readings, operational status, alerts) that needs to be ingested, processed, and analyzed with minimal latency to enable immediate anomaly detection and trigger timely maintenance actions.

### Challenge
The organization faces delayed anomaly detection due to hourly processing, limited scalability from VM-based workloads, inconsistent downstream data caused by weak ingestion validation, escalating cloud costs from over-provisioned static VMs, and governance gaps stemming from the absence of a centralized data catalog and unified security model.

### Solution
As a Data Engineer, I built an Real-time Lakehouse Medallion Architecture using ADLS Gen2 and Azure Databricks. The design is highly scalable and can handle high-volume IoT telemetry data efficiently while being cost-effective and providing a governed foundation for advanced analytics.

### Details
The data is ingested from IoT telemetry data into Azure Event Hubs and uploaded to blob storage through Azure Functions. Here we are utilizing Azure Databricks as the primary processing engine. Using PySpark and Delta Lake, I built ingestion pipelines that automatically load, cleanse, and transform the raw telemetry into the Medallion architecture in near real-time.

All the files from the Azure Events Hubs are pushed to blob storage via Azure Functions and they land in the following directory. The 'telemetry-data' is the path where data is pushed on regular interval and a timestamp is suffixed to the filename.
 ADLS             |  Blob files
:-------------------------:|:-------------------------:
<img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/23d6b1d3-b0b0-4cb9-92d1-205d37ecc208" />  |  <img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/07c9177c-2f8e-418d-a1cc-87ddd89bc94e" />

The config file serves as a one stop shop where we can update the parameters of our pipeline. The telemetry job is the backbone of this pipeline which continously runs and ingests new data from the blob path and updates the bronze telemetry table.
 Config file             |   Telemetry real-time Jobs
:-------------------------:|:-------------------------:
<img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/9f68532a-bc9b-4cad-a620-2430c0d7a475" />  |  <img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/86a9bc63-532d-4562-8034-8e1c8307bff5" />

The below snippet shows the logic behind the telemetry job which is using AutoLoader in the backgroud which continously ingests data. The inclusion of schema evoulution function is helpful to avoid any failure in the pipeline once a new dimension is pushed-in and which is rescued during the pipeline run. Whereas the analytical pipieline run is dependent on the bronze table updates, it runs as soon as the table is updated with any new data. 
Bronze ingestion using AutoLoader             |  Analytical Job 
:-------------------------:|:-------------------------:
<img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/5e7e14c8-5419-445d-ada0-ebd955c85315" />  |  <img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/8fd2ea05-a094-4907-91cb-0ecb3872d9fc" />

The logic for the analytical job is as below where we are cleaning the duplicates, correcting the schema and finally normalizing any values that are mathematically impossible. In gold job - we are mainly working on the data enrichment and aggregation for the BI workloads.
 Silver Job - cleaning and normalizing             |  Gold Job - aggregation and enrichment
:-------------------------:|:-------------------------:
<img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/94b5579e-3f2e-4a04-b17a-e94b7f8e5327" />  |  <img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/8d8c4e2e-7654-4ad7-9284-6f3dae33746c" />

The below shows the two pipelines - (1) bronze ingestion (2) analytical pipeline which are decoupled from each other - making the architecture scalable. The main idea is simply to run the ingestion continously and update the analytical tables as soon as the data is ingested.
 Pipeline Flow             |  Data Lineage
:-------------------------:|:-------------------------:
<img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/1b975f3f-d825-4315-813b-7320a2083833" />  |  <img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/34260624-761c-4742-aacf-e6b7c0e97bbf" />




























