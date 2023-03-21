import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import boto3
import re
import csv
import json
from datetime import date
from pyspark.sql.types import *
from helpers import check_inputs,RTTVerifyPhoneAndEmail,removeDuplicateColNames,FindLatestPrefix,createConfigDF,createDynamicFrameFromS3,writeDynamicFrameToS3,TrimFields

# main params
todays_date =str(date.today()).replace("-","/")
args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_bucket", "quarantine_bucket", "trusted_bucket","source_system_name","source_system_table_name","source_database_instance_name", "cross_account_event_bus", "glue_scripts_bucket"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"] + str(todays_date))

# params
system_name = args["source_system_name"].lower()
db_instance_name = args["source_database_instance_name"].lower()
source_system_table_name = args["source_system_table_name"].lower()
config_bucket_name = args["glue_scripts_bucket"].lower()
s3_config_file_name = 'sttm_config.csv'
s3_client = boto3.client('s3')
source_bucket = args['source_bucket'] #'datalake-raw-510716259290'
config_file_path = "{}/{}".format(config_bucket_name,s3_config_file_name)

print("SYSTEM NAME: ", system_name)

# assert user inputs are valid
check_inputs(config_bucket_name, s3_config_file_name, args["cross_account_event_bus"], args["source_system_name"],args["source_system_table_name"], args["source_database_instance_name"], args["JOB_NAME"])

# setup config/lookup table
configSparkDF = createConfigDF(config_file_path,glueContext)
configSparkDF.show(truncate=False)

# create collection - source_system_table_name is none for glue jobs
if args["source_system_table_name"] != "none":
    distinctSystemTablesCollection = configSparkDF.select('source_system',"schema","source_table").distinct().filter((configSparkDF['source_system'] == system_name) & (configSparkDF['source_table'] == args["source_system_table_name"])).collect()
else:
    distinctSystemTablesCollection = configSparkDF.select('source_system',"schema","source_table").distinct().filter(configSparkDF['source_system'] == system_name).collect()
        
print("Collection: ", distinctSystemTablesCollection)

# data cleanup
    
for row in distinctSystemTablesCollection:
    
    if row['schema'] == 'null': # non-glue systems
        s3_table_prefix = "{}/{}/{}".format(row['source_system'], row['source_table'], todays_date)
    else:
        concat_tab_name = "{}_{}_{}".format(db_instance_name.replace("/","_"), row['schema'], row['source_table'])
        s3_table_prefix = "{}/{}/{}".format(row['source_system'], concat_tab_name, todays_date)

    latest_s3_table_prefix = FindLatestPrefix(s3_client, source_bucket, s3_table_prefix)
        
    results = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=latest_s3_table_prefix)
    s3_table_prefix_exists = 'Contents' in results
    
    # get file extension
    file_extension=""
    try:
        first_key = results['Contents'][0]['Key']
        extension_index = first_key.rindex('.') + 1
        file_extension = first_key[extension_index:]
    except:
        file_extension="parquet"
        
    if s3_table_prefix_exists:

        quarantine_bucket = args['quarantine_bucket'] #'non-trusted-data-quarantine'
        trusted_bucket = args['trusted_bucket'] #'datalake-trusted-NNNNNNNNN

        print("processing for: ", row['source_system'], ":", row['source_table'])
        target_table_folder = "{}/{}".format(source_bucket, latest_s3_table_prefix)
        quarantine_table_folder = "{}/{}".format(quarantine_bucket, latest_s3_table_prefix)
        trusted_table_folder = "{}/{}".format(trusted_bucket, latest_s3_table_prefix)

        print("table bucket location: ", target_table_folder)
        print("quarantine bucket location: ", quarantine_table_folder)
        print("trusted bucket location: ", trusted_table_folder)

        # convert dynamic frame to spark dataframe and perform basic data cleaning
        sparkDF = createDynamicFrameFromS3(target_table_folder,glueContext,file_extension)
        print("DF starting row count: ", sparkDF.count())

        # special case for API based ingestion source systems
        if system_name in ['iterable_api','splash_that','golf_genius']:
            # replace dots with underscores to remove special char logic if exists
            sparkDF = sparkDF.toDF(*(c.replace('.', '_') for c in sparkDF.columns))
            # remove duplicate column names that may exist (only for json file source systems)
            sparkDF = removeDuplicateColNames(sparkDF)

        # dropping duplicate and null records
        remDupsDF = sparkDF.dropDuplicates().dropna(how='all')
        print("DF with duplicates and null records removed count: ", remDupsDF.count())


        # get table columns
        table_cols = configSparkDF.filter((col("source_system") == row["source_system"]) & (col("source_table") == row["source_table"] )).select("source_column").rdd.map(lambda x: x.source_column).collect()

        # trim all target fields
        trimmedDF = TrimFields(remDupsDF, table_cols)

        # cleanup email and phone cols
        valid_df,invalid_df = RTTVerifyPhoneAndEmail(trimmedDF, table_cols, system_name, spark)

        # convert back to dynamicframe
        valid_df_end = DynamicFrame.fromDF(valid_df, glueContext, 'valid_data')
        invalid_df_end = DynamicFrame.fromDF(invalid_df,glueContext, 'invalid_data')

        # write invalid dataframe to Quarantine bucket
        writeDynamicFrameToS3(quarantine_table_folder, invalid_df_end, glueContext, 'quarantine_bucket')

        # write valid dataframe to Trusted bucket
        writeDynamicFrameToS3(trusted_table_folder, valid_df_end, glueContext, 'trusted_bucket')
        print('{}:RTT Successful for System: {}:{}'.format(todays_date,system_name,source_system_table_name))
        
    else:
        print(s3_table_prefix, "does not exists")
        
job.commit()