import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
import json
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import Filter, ApplyMapping
from awsgluedq.transforms import EvaluateDataQuality
from pyspark.sql.functions import col, lit, abs, greatest, least, round
from awsglue.dynamicframe import DynamicFrame
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
sourceDBName = secrets.get("rawDB")
sourceTBName = secrets.get("rawTB")
destinationDBName = secrets.get("processedDB")
destinationTBName = secrets.get("cruatedTB")
destinationPath = secrets.get("curatedPath")


# Read Job Args
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Start Spark + Glue Context
spark_context = SparkContext()
glue_context = GlueContext(spark_context)
spark = glue_context.spark_session

# Configure Iceberg Catalog
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", destinationPath)
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

# Initialize Job
job = Job(glue_context)
job.init(args['JOB_NAME'], args)


# Reading Raw IoT Data From Catalog Table
raw_iot_dyf = glue_context.create_dynamic_frame.from_catalog(
    database=sourceDBName,
    table_name=sourceTBName,
    transformation_ctx="raw_iot_dyf"
)

# Apply Schema 
mapped_iot_dyf = ApplyMapping.apply(
    frame=raw_iot_dyf,
    mappings=[
        ("device_id", "string", "device_id", "string"),
        ("timestamp", "string", "timestamp", "timestamp"),
        ("sensor_temp", "double", "sensor_temp", "double"),
        ("sensor_vibration", "double", "sensor_vibration", "double"),
        ("sensor_pressure", "double", "sensor_pressure", "double"),
        ("status_code", "string", "status_code", "string"),
        ("year", "string", "year", "string"),
        ("month", "string", "month", "string"),
        ("day", "string", "day", "string"),
        ("hour", "string", "hour", "string")
    ],
    transformation_ctx="mapped_iot_dyf"
)

# Filter Out Negative Values
validated_iot_dyf = Filter.apply(
    frame=mapped_iot_dyf,
    f=lambda row: (
        row["sensor_temp"] >= 0 and 
        row["sensor_vibration"] >= 0 and 
        row["sensor_pressure"] >= 0
    ),
    transformation_ctx="validated_iot_dyf"
)


validated_iot_df = validated_iot_dyf.toDF()

# Compute Health Score
validated_iot_df = validated_iot_df.withColumn(
    "health_score",
    round(
        greatest(
            lit(0.0),
            least(
                lit(100.0),
                100.0
                - (abs(col("sensor_temp") - 50) * 0.5)
                - (abs(col("sensor_vibration") - 5) * 5)
                - (abs(col("sensor_pressure") - 65) * 0.3)
            )
        ),
        2
    )
)

# Writing To Destination
destinationDB = destinationDBName
DestinationTable = destinationTBName

available_tables = spark.catalog.listTables(destinationDB)
existing_table_names = [t.name for t in available_tables]
is_table_existing = DestinationTable in existing_table_names

write_path = "glue_catalog." + destinationDB + "." + DestinationTable

if is_table_existing:
    (
        validated_iot_df
        .sortWithinPartitions("year", "month", "day", "hour")
        .writeTo(write_path)
        .tableProperty("format-version", "2")
        .tableProperty("location",destinationPath)
        .tableProperty("write.parquet.compression-codec", "uncompressed")
        .append()
    )
else:
    (
        validated_iot_df
        .sortWithinPartitions("year", "month", "day", "hour")
        .writeTo(write_path)
        .tableProperty("format-version", "2")
        .tableProperty("location", destinationPath)
        .tableProperty("write.parquet.compression-codec", "uncompressed")
        .create()
    )


job.commit()
