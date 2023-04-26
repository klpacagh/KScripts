import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import json
import csv
from pyspark.sql import SparkSession, Row
from awsglue.dynamicframe import DynamicFrame
from datetime import date, timedelta
from helpers import writeDynamicFrameToS3,createDynamicFrameFromS3

# main params
todays_date = str(date.today()).replace('-','/')
yesterday = str(date.today() - timedelta(days=1)).replace('-','/')
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_system_name','source_system_table_name','trusted_bucket','catalog_db_name'])
trusted_bucket = args['trusted_bucket']
system_name = args['source_system_name']
system_table_name = args['source_system_table_name']
catalog_db_name = args['catalog_db_name']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
job = Job(glueContext)
job.init(args["JOB_NAME"] + str(todays_date))
job.commit()

# begin process

# grab first file from trusted bucket
s3_client = boto3.client('s3')
prefix_file = '{}/{}/{}'.format(system_name,system_table_name,yesterday)
results = s3_client.list_objects_v2(Bucket=trusted_bucket, Prefix=prefix_file, MaxKeys=5)
print("prefix file: ", prefix_file)
print("results: ", results)

# grab the first key file in the bucket
file_name = '{}/{}'.format(trusted_bucket,results['Contents'][0]['Key']) 

# create dataframe to grab schema
temp_df = createDynamicFrameFromS3(file_name, glueContext,'parquet')

# create dummy frame
dummyDF = spark.createDataFrame([(None,) * len(temp_df.schema)],temp_df.schema)

# write dummy frame
trusted_table_folder = "s3://{}/{}/{}/{}".format(trusted_bucket, system_name, system_table_name, todays_date)
dummyDF.write.format("parquet").mode("overwrite").option("compression", "snappy").save(trusted_table_folder)
print('Writing Dummy File For System: {}:{} At Location: {}'.format(system_name,system_table_name,trusted_table_folder))

'''
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import json
import csv
from pyspark.sql import SparkSession, Row
from awsglue.dynamicframe import DynamicFrame
from datetime import date
from helpers import writeDynamicFrameToS3,createDynamicFrameFromS3

# main params
todays_date =str(date.today()).replace("-","/")
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_system_name','source_system_table_name','trusted_bucket','catalog_db_name'])
trusted_bucket = args['trusted_bucket']
system_name = args['source_system_name']
system_table_name = args['source_system_table_name']
catalog_db_name = args['catalog_db_name']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"] + str(todays_date))
job.commit()

# begin process

# grab first file from trusted bucket
s3_client = boto3.client('s3')
prefix_file = '{}/{}'.format(system_name,system_table_name)
results = s3_client.list_objects_v2(Bucket=trusted_bucket, Prefix=prefix_file, MaxKeys=5)
print("prefix file: ", prefix_file)
print("results: ", results)

# grab the first key file in the bucket
file_name = '{}/{}'.format(trusted_bucket,results['Contents'][0]['Key']) 

# create dataframe to grab schema
temp_df = createDynamicFrameFromS3(file_name, glueContext,'parquet')

# create dummy record
record_dict = dict()
for i in temp_df.schema.fields:
    temp = str(i.dataType)
    if 'ArrayType' in temp.split('('):
        record_dict[i.name] = ['None']
    else:
        record_dict[i.name] = 'None'

# create dummy frame
dummy_record = []
dummy_record.append(record_dict)
dummyDF = spark.createDataFrame([Row(**i) for i in dummy_record])

# write dummy frame
trusted_table_folder = "{}/{}/{}/{}".format(trusted_bucket, system_name, system_table_name, todays_date)
tempDF = DynamicFrame.fromDF(dummyDF, glueContext, 'temp')
writeDynamicFrameToS3(trusted_table_folder, tempDF, glueContext, transf_ctx='temp_ctxz')
print('Writing Dummy File For System: {}:{} At Location: {}'.format(system_name,system_table_name,trusted_table_folder))
'''