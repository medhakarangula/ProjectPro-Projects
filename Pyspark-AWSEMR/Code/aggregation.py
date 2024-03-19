from pyspark.sql import SparkSession

S3_DATA_INPUT_PATH="s3://s3-projectpro-emr-athena/source-folder/wikiticker-2015-09-12-sampled.json"
S3_DATA_OUTPUT_PATH_AGGREGATED="s3://s3-projectpro-emr-athena/data-output/aggregated"

def main():
    spark = SparkSession.builder.appName('projectProDemo1').getOrCreate()
    df = spark.read.json(S3_DATA_INPUT_PATH)
    print(f'The total number of records in the input data set is {df.count()}')
    aggregated_df = df.groupBy(df.channel).count()
    print(f'The total number of records in the aggregated data set is {aggregated_df.count()}')
    aggregated_df.show(10)
    aggregated_df.printSchema()
    aggregated_df.write.mode('overwrite').parquet(S3_DATA_OUTPUT_PATH_AGGREGATED)
    print('The aggregated data has been uploaded successfully')


if __name__ == '__main__':
    main()