import sys
from awsglue.transforms import *
from awsglue.utils import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

# Initialize SparkContext
sc = SparkContext()

# Initialize GlueContext
glueContext = GlueContext(sc)

# Initialize SparkSession
spark = glueContext.spark_session

# Read source table
source_frame = glueContext.create_dynamic_frame.from_catalog(database = "unsw_nb15", table_name = "unsw_nb15")

# Apply SQL query
query_results_frame = source_frame.toDF().createOrReplaceTempView("temp_table")
query_results = spark.sql("SELECT * FROM temp_table WHERE proto IN ('tcp', 'udp')")

# Create a new DynamicFrame
proto_filtered_data = DynamicFrame.fromDF(query_results, glueContext, "proto_filtered_data")
glueContext.write_dynamic_frame.from_catalog(frame = proto_filtered_data, database = "unsw_nb15", table_name = new_table_name)

# Repartition the data to a single output file (optional)
proto_filtered_data = proto_filtered_data.repartition(1)

# Write the filtered data to an S3 bucket in CSV format
output_s3_path = "s3://p50-bigdata-proto-filtered/unsw-nb15/proto_filtered/"
glueContext.write_dynamic_frame.from_options(
    frame=proto_filtered_data,
    connection_type="s3",
    connection_options={"path": output_s3_path},
    format="csv",
    format_options={"writeHeader": True}  # Optional, write column headers in CSV
)

