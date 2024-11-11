import sys
from pyspark.context import SparkContext
import json
import boto3
from urllib.parse import urlparse
from io import StringIO
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import from_json, col, to_json, lit, udf, length, when, encode, expr, coalesce
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType, TimestampType, LongType, DoubleType

aws_region = "us-west-2"
checkpoint_interval = 60
checkpoint_location = "s3://examples3/check_point/"
consumer_group = "group1"
kafka_broker = "kafkahost:9092"
startingOffsets = "earliest"

redshift_host = "redshiftendpoint"
redshift_port = "5439"
redshift_username = "xxx"
redshift_password = "xxx"
redshift_database = "dev"
redshift_schema = "public"
redshift_table = "xxx"
# emr以s3作为中转，存入csv在s3，然后redshift 从s3 copy数据
redshift_tmpdir = "s3://examples3/redshift_tmp/"
tempformat = "CSV"
redshift_iam_role = "arn:aws:iam::128761886083:role/redshift-admin"

spark = SparkSession.builder \
.config('spark.scheduler.mode', 'FAIR') \
.getOrCreate()
sc = spark.sparkContext
maxerror = 0

topic = "mytopic5"

kafka_df = (
    spark
    .read
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_broker)
    .option("subscribe", topic)
    .option("kafka.consumer.commit.groupid", consumer_group)
    .option("startingOffsets", "earliest") \
    .option("kafka.security.protocol", "PLAINTEXT") \
    .load()
)

kafka_json_df = kafka_df.withColumn("value", expr("CAST(value AS STRING)"))

schema = StructType([
    StructField("fields", StructType([
        StructField("event_type", ArrayType(StringType()), True),
        StructField("src_process_publisher", ArrayType(StringType()), True),
        StructField("src_process_cmdline", ArrayType(StringType()), True),
        StructField("src_process_name", ArrayType(StringType()), True),
        StructField("tgt_process_publisher", ArrayType(StringType()), True),
        StructField("tgt_process_cmdline", ArrayType(StringType()), True),
        StructField("tgt_process_name", ArrayType(StringType()), True),
        StructField("endpoint_name", ArrayType(StringType()), True),
        StructField("tgt_process_displayName", ArrayType(StringType()), True),
        StructField("src_process_displayName", ArrayType(StringType()), True),
        StructField("endpoint_os", ArrayType(StringType()), True),
        StructField("src_process_parent_name", ArrayType(StringType()), True),
        StructField("src_process_parent_cmdline", ArrayType(StringType()), True),
        StructField("src_process_parent_publisher", ArrayType(StringType()), True),
        StructField("event_dns_request", ArrayType(StringType()), True),
        StructField("url_address", ArrayType(StringType()), True),
        StructField("agent_uuid", ArrayType(StringType()), True),
        StructField("src_process_image_sha1", ArrayType(StringType()), True),
        StructField("tgt_process_image_sha1", ArrayType(StringType()), True),
        StructField("src_process_parent_image_sha1", ArrayType(StringType()), True),
        StructField("task_path", ArrayType(StringType()), True),
        StructField("tgt_file_extension", ArrayType(StringType()), True),
        StructField("dst_ip_address", ArrayType(StringType()), True),
        StructField("indicator_category", ArrayType(StringType()), True),
        StructField("indicator_description", ArrayType(StringType()), True),
        StructField("indicator_metadata", ArrayType(StringType()), True),
        StructField("indicator_name", ArrayType(StringType()), True),
        StructField("dst_port_number", ArrayType(StringType()), True),
        StructField("event_network_connectionStatus", ArrayType(StringType()), True),
        StructField("tgt_process_image_path", ArrayType(StringType()), True),
        StructField("src_process_parent_image_path", ArrayType(StringType()), True),
        StructField("src_process_image_path", ArrayType(StringType()), True),
        StructField("tgt_file_sha1", ArrayType(StringType()), True),
        StructField("event_dns_response", ArrayType(StringType()), True),
        # Add more fields as needed based on the JSON structure
    ]), True),
    StructField("sort", ArrayType(LongType(), True), True)
])

streaming_df = kafka_json_df.withColumn("values_json", from_json(col("value"), schema)).selectExpr("values_json.*")

# Function to split src_process_cmdline if it's larger than 65535 bytes
def split_src_process_cmdline(src_process_cmdline):
    if src_process_cmdline and len(src_process_cmdline[0].encode('utf-8')) > 65535:
        return (src_process_cmdline[0][:65535], src_process_cmdline[0][65535:])
    return (src_process_cmdline[0] if src_process_cmdline else None, "")

split_udf = udf(split_src_process_cmdline, StructType([
    StructField("src_process_cmdline", StringType(), True),
    StructField("src_process_cmdline_overflow", StringType(), True)
]))

flattened_df = streaming_df.select(
    col("fields.event_type"),
    col("fields.src_process_publisher"),
    col("fields.src_process_name"),
    col("fields.tgt_process_publisher"),
    col("fields.tgt_process_cmdline"),
    col("fields.tgt_process_name"),
    col("fields.endpoint_name"),
    col("fields.tgt_process_displayName"),
    col("fields.src_process_displayName"),
    col("fields.endpoint_os"),
    col("fields.src_process_parent_name"),
    col("fields.src_process_parent_cmdline"),
    col("fields.src_process_parent_publisher"),
    col("fields.event_dns_request"),
    col("fields.url_address"),
    col("fields.agent_uuid"),
    col("fields.src_process_image_sha1"),
    col("fields.tgt_process_image_sha1"),
    col("fields.src_process_parent_image_sha1"),
    col("fields.task_path"),
    col("fields.tgt_file_extension"),
    col("fields.dst_ip_address"),
    col("fields.indicator_category"),
    col("fields.indicator_description"),
    col("fields.indicator_metadata"),
    col("fields.indicator_name"),
    col("fields.dst_port_number"),
    col("fields.event_network_connectionStatus"),
    col("fields.tgt_process_image_path"),
    col("fields.src_process_parent_image_path"),
    col("fields.src_process_image_path"),
    col("fields.tgt_file_sha1"),
    col("fields.event_dns_response"),
    split_udf(col("fields.src_process_cmdline")).alias("split_src_process_cmdline"),
    col("sort")
).select(
    "*",
    col("split_src_process_cmdline.src_process_cmdline"),
    col("split_src_process_cmdline.src_process_cmdline_overflow")
).drop("split_src_process_cmdline")

# Ensure src_process_cmdline_overflow is always present
flattened_df = flattened_df.withColumn(
    "src_process_cmdline_overflow",
    when(col("src_process_cmdline_overflow").isNull(), "").otherwise(col("src_process_cmdline_overflow"))
)

#把flattend_df里面的keyvalue array改成string
exp_df = flattened_df.select([
    (col(c)[0] if isinstance(flattened_df.schema[c].dataType, ArrayType) else col(c)).alias(c)
    for c in flattened_df.columns
])

#通过coalesce方法，把所有null字段的column 填充""，保证写入redshift不会报错。（null 写入redshift会报错）
final_df = exp_df.select([coalesce(col(c), lit("")).alias(c) for c in exp_df.columns])

final_df.createOrReplaceGlobalTempView("internal_source_table")

columnMap = {
            "event_type": "event_type",
            "src_process_publisher": "src_process_publisher",
            "src_process_cmdline": "src_process_cmdline",
            "src_process_cmdline_overflow": "src_process_cmdline_overflow",
            "src_process_name": "src_process_name",
            "tgt_process_publisher": "tgt_process_publisher",
            "tgt_process_cmdline": "tgt_process_cmdline",
            "tgt_process_name": "tgt_process_name",
            "endpoint_name": "endpoint_name",
            "tgt_process_displayName": "tgt_process_displayName",
            "src_process_displayName": "src_process_displayName",
            "endpoint_os": "endpoint_os",
            "src_process_parent_name": "src_process_parent_name",
            "src_process_parent_cmdline": "src_process_parent_cmdline",
            "src_process_parent_publisher": "src_process_parent_publisher",
            "event_dns_request": "event_dns_request",
            "url_address": "url_address",
            "agent_uuid": "agent_uuid",
            "src_process_image_sha1": "src_process_image_sha1",
            "tgt_process_image_sha1": "tgt_process_image_sha1",
            "src_process_parent_image_sha1": "src_process_parent_image_sha1",
            "task_path": "task_path",
            "tgt_file_extension": "tgt_file_extension",
            "dst_ip_address": "dst_ip_address",
            "indicator_category": "indicator_category",
            "indicator_description": "indicator_description",
            "indicator_metadata": "indicator_metadata",
            "indicator_name": "indicator_name",
            "tgt_file_extension": "tgt_file_extension",
            "dst_ip_address": "dst_ip_address",
            "indicator_category": "indicator_category",
            "indicator_description": "indicator_description",
            "indicator_metadata": "indicator_metadata",
            "indicator_name": "indicator_name",
            "dst_port_number": "dst_port_number",
            "event_network_connectionStatus": "event_network_connectionStatus",
            "tgt_process_image_path": "tgt_process_image_path",
            "src_process_parent_image_path": "src_process_parent_image_path",
            "src_process_image_path": "src_process_image_path",
            "tgt_file_sha1": "tgt_file_sha1",
            "event_dns_response": "event_dns_response"
        }

select_expr = [f"{original} AS {new}" for original, new in columnMap.items()]
query = f"SELECT {', '.join(select_expr)} FROM global_temp.internal_source_table"
csdf = spark.sql(query)
csdf.write \
    .format("io.github.spark_redshift_community.spark.redshift") \
    .option("url", "jdbc:redshift://{0}:{1}/{2}".format(redshift_host, redshift_port, redshift_database)) \
    .option("dbtable", "{0}.{1}".format(redshift_schema, redshift_table)) \
    .option("user", redshift_username) \
    .option("password", redshift_password) \
    .option("tempdir", redshift_tmpdir) \
    .option("tempformat", tempformat) \
    .option("tempdir_region", aws_region) \
    .option("aws_iam_role", redshift_iam_role).mode("append").save()
