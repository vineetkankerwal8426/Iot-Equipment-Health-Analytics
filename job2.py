import sys
import json
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, avg, stddev, sum as _sum, when,lit, greatest, least, round,count
import boto3
from botocore.exceptions import ClientError

secrets_client = boto3.client("secretsmanager")

def get_secrets():
    secret_name = "iot-secrets"
    try:
        get_secret_value_response = secrets_client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print(f"The secret {secret_name} was not found")
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            print(f"The request was invalid due to:", e)
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            print(f"The request had invalid params:", e)
            raise e
        else:
            raise e
    secret = get_secret_value_response['SecretString']
    secret_data = json.loads(secret) 
    return secret_data


secrets = get_secrets()
sourceTBName = secrets.get("curatedTB")
DBName = secrets.get("processedDB")
destinationTBName = secrets.get("goldenTB")
destinationPath = secrets.get("goldenPath")


args = getResolvedOptions(sys.argv, ["JOB_NAME"])

spark_context = SparkContext()
glue_context = GlueContext(spark_context)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)



spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", destinationPath)

# Reading Curated Table (Silver Layer)

tablePath = DBName.SourceTBName

curated_sensor_data_df = spark.table(tablePath)

# Hourly Aggregation – Gold Layer Transformation

hourly_aggregated_df = (
    curated_sensor_data_df
        .groupBy(["device_id", "year", "month", "day", "hour"])
        .agg(
            avg("sensor_temp").alias("avg_temp"),
            avg("sensor_vibration").alias("avg_vibration"),
            avg("sensor_pressure").alias("avg_pressure"),
            stddev("sensor_temp").alias("std_temp"),
            stddev("sensor_vibration").alias("std_vibration"),
            stddev("sensor_pressure").alias("std_pressure"),
            avg("health_score").alias("avg_health_score"),
            count("*").alias("reading_count"),
            _sum(when(col("status_code") == "OK", 1).otherwise(0)).alias("ok_count"),
            _sum(when(col("status_code") == "WARN", 1).otherwise(0)).alias("warn_count"),
            _sum(when(col("status_code") == "FAIL", 1).otherwise(0)).alias("fail_count")
        )
)

# Anomaly Flag Based on Rules

hourly_aggregated_df = (
    hourly_aggregated_df.withColumn(
        "anomaly_flag",
        when((col("fail_count") >= 8) & (col("warn_count") >= 4), lit(1))
        .otherwise(lit(0))
    )
)



database = DBName
destinationTable = destinationTBName
gold_table_identifier = f"glue_catalog.{database}.{destinationTable}"

existing_tables = spark.catalog.listTables(database)
existing_table_names = [tbl.name for tbl in existing_tables]
table_exists = destinationTable in existing_table_names

# Writing Hourly Aggregates Into Gold Layer Table

location = destinationPath

if table_exists:
    (
        hourly_aggregated_df
        .sortWithinPartitions("year", "month", "day")
        .writeTo(gold_table_identifier)
        .tableProperty("format-version", "2")
        .tableProperty("location", location)
        .tableProperty("write.parquet.compression-codec", "uncompressed")
        .append()
    )
else:
    (
        hourly_aggregated_df
        .sortWithinPartitions("year", "month", "day")
        .writeTo(gold_table_identifier)
        .tableProperty("format-version", "2")
        .tableProperty("location", location)
        .tableProperty("write.parquet.compression-codec", "uncompressed")
        .partitionedBy("year", "month", "day")
        .create()
    )

job.commit()
