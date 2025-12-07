# PySpark Example Project for real time trades data lake house analytics

This document is designed to be read in parallel with the code in the `pyspark-template-project` repository. Together, these constitute what we consider to be a 'best practices' approach to writing ETL jobs using Apache Spark and its Python ('PySpark') APIs. This project addresses the following topics:

- how to structure ETL code in such a way that it can be easily tested and debugged;
- how to pass configuration parameters to a PySpark job;
- how to handle dependencies on other modules and packages; and,
- what constitutes a 'meaningful' test for an ETL job.

## ETL Project Structure

The basic project structure is as follows:

```bash
root/
 |-- configs/
 |   |-- etl_config.json
 |-- dependencies/
 |   |-- logging.py
 |   |-- spark.py
 |-- sql/
 |   |-- ddls.sql
 |-- jobs/
 |   |-- etl_job_bronze_silver.py
 |   |-- etl_job_silver_gold.py
 |   build_dependencies.sh
 |   packages.zip
 |   Pipfile
 |   Pipfile.lock
```




----Steps for running

1. Run DDLs via spark-sql or the Snowflake SQL console to create tables in each layer
2. Configure Spark with Iceberg Connectors by adding icevberg connector jars
3. run spark-submit silver_layer_processing.py -> on ssh
4. When source is changed from sample data to kafka & instead of show to actually writing in iceberg table
   a. write kafka source creds and related properties in etl_config.json
   b. write s3 + iceberg + snowflake open catalog properties in etl_config.json and use iceberg_bronze_silver.py and iceberg_silver_gold.py files from the project
