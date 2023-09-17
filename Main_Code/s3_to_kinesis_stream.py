import boto3
import csv
import json
import dateutil.parser as parser
from time import sleep
from datetime import datetime

# AWS SDK Local Configuration
aws_access_key_id = "1234"
aws_secret_access_key = "****"
region_name = "us-east-2"

# Create S3 Client with Custom Credentials
s3 = boto3.client('s3', region_name=region_name,
                  aws_access_key_id=aws_access_key_id,
                  aws_secret_access_key=aws_secret_access_key)

# Create S3 Resource with Custom Credentials
s3_resource = boto3.resource('s3', region_name=region_name,
                              aws_access_key_id=aws_access_key_id,
                              aws_secret_access_key=aws_secret_access_key)

# Create Kinesis Client with Custom Credentials
kinesis_client = boto3.client('kinesis', region_name=region_name,
                              aws_access_key_id=aws_access_key_id,
                              aws_secret_access_key=aws_secret_access_key)


# Env. variables; i.e. can be OS variables in Lambda
kinesis_stream_name = 'p50_raw_attack_records_stream'
#Partition key not needed as dataset is small
streaming_partition_key = 'service'


# Function can be converted to Lambda; 
#   i.e. by iterating the S3-put events records; e.g. record['s3']['bucket']['name']
def stream_data_simulator(input_s3_bucket, input_s3_key):
  s3_bucket = input_s3_bucket
  s3_key = input_s3_key
  
  # Read CSV Lines and split the file into lines
  csv_file = s3_resource.Object(s3_bucket, s3_key)
  s3_response = csv_file.get()
  lines = s3_response['Body'].read().decode('utf-8').split('\n')
  
  for row in csv.DictReader(lines):
      try:
          # Convert to JSON, to make it easier to work in Kinesis Analytics
          line_json = json.dumps(row)
          json_load = json.loads(line_json)
          # Adding fake txn ts:
          json_load['Txn_Timestamp'] = datetime.now().isoformat()
          
          # Write to Kinesis Streams:
          response = kinesis_client.put_record(StreamName=kinesis_stream_name,Data=json.dumps(json_load, indent=4),PartitionKey=str(json_load[streaming_partition_key]))
          print("****************************************************",response)
          
          # Adding a temporary pause, for demo-purposes:
          sleep(0.250)
          
      except Exception as e:
          print('Error: {}'.format(e))
          print("*************************************************")

# Run stream:
for i in range(0, 3):
  stream_data_simulator(input_s3_bucket="p50-bigdata-filtered-attack-records", input_s3_key="unsw-nb15/p50_attack_records/part-00000-373491ed-0423-427e-90c5-d88a661449a0-c000.csv")
