import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import date
import boto3

# date
todays_date = date.today()

# setup glue job
args = getResolvedOptions(sys.argv, ["JOB_NAME", "system_name", "database_name", "s3_raw_bucket_path"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# get glue database
client = boto3.client('glue',region_name='us-east-1')

next_token = ""

while True:
    glue_db = client.get_tables(DatabaseName=args['database_name'],NextToken=next_token)

    for table in glue_db.get('TableList'):
        table_name = table.get('Name')
                
                # read from glue into dynamic frame
        read_node = glueContext.create_dynamic_frame.from_catalog(
                database=args['database_name'],
                table_name=table_name,
                transformation_ctx= table_name + "_crctx",
                )

                # write dynamic frame to s3
        write_node = glueContext.write_dynamic_frame.from_options(
                frame=read_node,
                connection_type="s3",
                format="glueparquet",
                connection_options={"path": args['s3_raw_bucket_path'] + args['system_name'] + "/{}/{}/{}/{}".format(table_name,todays_date.year,todays_date.month,todays_date.day),
                         "partitionKeys": []},
                transformation_ctx= table_name + "_wrctx",
                )

    next_token = glue_db.get('NextToken')

    if next_token is None:
        break


job.commit()
