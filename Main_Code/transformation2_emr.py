from pyspark.sql import SparkSession

S3_DATA_INPUT_PATH = "s3://p50-bigdata-proto-filtered/unsw-nb15/proto_filtered/run-1693765110601-part-r-00000"
S3_DATA_OUTPUT_PATH = "s3://p50-bigdata-filtered-attack-records/unsw-nb15/p50_attack_records"

def main():
    spark = SparkSession.builder.appName('p50_projectProDemo').getOrCreate()
    
    # Read CSV dataset with header
    df = spark.read.option("header", "true").csv(S3_DATA_INPUT_PATH)
    
    # Display the schema of the DataFrame
    df.printSchema()
    
    # Filter records where Label equals 1
    filtered_df = df.filter(df.Label == '1')
    
    print(f'****************The total number of records in the input data set is {df.count()}******************')
    print(f'****************The total number of records in the filtered data set is {filtered_df.count()}************')
    
    filtered_df.show(10)  # Display the first 10 records of the filtered DataFrame
    
    # Write the filtered DataFrame to S3 in CSV format
    filtered_df.write.mode('overwrite').option("header", "true").csv(S3_DATA_OUTPUT_PATH)
    print('**************************The filtered data has been uploaded successfully in CSV format******************')

if __name__ == '__main__':
    main()
