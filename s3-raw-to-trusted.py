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
from datetime import date
from pyspark.sql.types import *

todays_date = date.today()

args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_bucket", "quarantine_bucket", "trusted_bucket","config_file_path","source_system_name","source_system_table_name"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"] + str(todays_date))
system_name = args["source_system_name"]

print("SYSTEM NAME: ", system_name)

def ValidatePhone(number):
    
    # checks for null value first
    if number is not None:
            
    # remove special characters        
        number = re.sub(r"[^0-9]", "", number)
        
    # check that phone is a 10 digit number without/without country code   
        if len(number) in range(10,13):
                return 'valid'
            
        else:
            return 'invalid'
                        
def ValidateEmail(email):
    top_level_domains = ["com","org","net","int","edu","gov","mil","biz"]
    
    # matches email string requirements based on RFC5322 standards
    main_match = re.search(r'([A-Za-z0-9]+[.-_])*[A-Za-z0-9]+@[A-Za-z0-9-]+(\.[A-Z|a-z]{2,})+', str(email))

    if main_match:
    
    # search for . after @ and verify top level domain 
    
        index_of_amperstand = email.rindex("@") + 1
        index_of_period = email.rindex(".", index_of_amperstand) + 1
        domain_sub = email[index_of_period:] # matches the domain
        if domain_sub in top_level_domains:
            return "valid"
        
    return "invalid"

# main params
s3_client = boto3.client('s3')
source_bucket = args['source_bucket'] #'datalake-raw-510716259290'
config_file_path = args['config_file_path'] #'pga-pipeline-scripts/sttm_config.csv'

todays_date =str(date.today()).replace("-","/")

# initialize UDFs 
emailValidateUDF = udf(lambda x: ValidateEmail(x), StringType())
phoneValidateUDF = udf(lambda x: ValidatePhone(x), StringType())

# setup config/lookup table
configDF = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="csv",
    format_options= {'withHeader': True},
    connection_options={"paths": ["s3://{}".format(config_file_path)], "recurse": True},
    transformation_ctx="config_node_ctx"
)

configSparkDF = configDF.toDF()
configSparkDF.show(truncate=False)

# looping through these collections
if "salesforce" in system_name:
    distinctSystemTablesCollection = configSparkDF.select('source_system',"schema","source_table").distinct().filter((configSparkDF['source_system'] == system_name) & (configSparkDF['source_table'] == args["source_system_table_name"])).collect()
else:
    distinctSystemTablesCollection = configSparkDF.select('source_system',"schema","source_table").distinct().filter(configSparkDF['source_system'] == system_name).collect()
    
    temp_bucket = s3_client.list_objects(Bucket=source_bucket, Prefix=system_name, MaxKeys=2)
    temp_bucket_record = temp_bucket['Contents'][1]['Key'].split('/')[1]
    parsed_db_instance_name = ""
        
print("object name: ", distinctSystemTablesCollection)

configCollection = configSparkDF.collect()

# data cleanup
    
for row in distinctSystemTablesCollection:
    
    if "salesforce" in row['source_system']:
        s3_table_prefix = "{}/{}/{}".format(row['source_system'], row['source_table'], todays_date)
    else:
        if parsed_db_instance_name == "":
            db_schema = '_' + row['schema'] + '_'
            index_of = temp_bucket_record.index(db_schema)
            parsed_db_instance_name = temp_bucket_record[:index_of]

        concat_tab_name = "{}_{}_{}".format(parsed_db_instance_name, row['schema'], row['source_table'])
        s3_table_prefix = "{}/{}/{}".format(row['source_system'], concat_tab_name, todays_date)
        
    results = s3_client.list_objects(Bucket=source_bucket, Prefix=s3_table_prefix)
    # print("results: ", results)
    s3_table_prefix_exists = 'Contents' in results
    
    if s3_table_prefix_exists:

        quarantine_bucket = args['quarantine_bucket'] #'non-trusted-data-quarantine'
        trusted_bucket = args['trusted_bucket'] #'datalake-trusted-510716259290'

        print("processing for: ", row['source_system'], ":", row['source_table'])
        target_table_folder = "{}/{}".format(source_bucket, s3_table_prefix)
        quarantine_table_folder = "{}/{}".format(quarantine_bucket, s3_table_prefix)
        trusted_table_folder = "{}/{}".format(trusted_bucket, s3_table_prefix)

        print("table bucket location: ", target_table_folder)
        print("quarantine bucket location: ", quarantine_table_folder)
        print("trusted bucket location: ", trusted_table_folder)

        rawDF = glueContext.create_dynamic_frame.from_options(
            format_options={},
            connection_type="s3",
            format="parquet",
            connection_options={"paths": ["s3://{}".format(target_table_folder)], "recurse": True},
            transformation_ctx="temp_node_ctx"
        )

        # convert dynamic frame to spark dataframe and perform basic data cleaning
        sparkDF = rawDF.toDF()
        print("df starting row count: ", sparkDF.count())
        # sparkDF_toLower = sparkDF.select([F.col(x).alias(x.lower()) for x in sparkDF.columns])
        # sparkDF_toLower = sparkDF.toDF(*[c.lower() for c in sparkDF.columns])
        no_dupes = sparkDF.dropDuplicates().dropna(how='all')
        print("df with dups removed  count: ", no_dupes.count())

        # get table columns
        table_cols = configSparkDF.filter((col("source_system") == row["source_system"]) & (col("source_table") == row["source_table"] )).select("source_column").rdd.map(lambda x: x.source_column).collect()
        # print(table_cols)

        # empty 
        valid_email_df = 0
        invalid_email_df = 0
        valid_phone_df = 0
        invalid_phone_df = 0
        email_pass = False
        phone_pass = False
        
        if "email" in table_cols:
            validation_df = no_dupes.withColumn('email_validation', emailValidateUDF(col('email')))
            valid_email_df = validation_df.filter(validation_df['email_validation'] == 'valid').drop('email_validation')
            invalid_email_df = validation_df.filter(validation_df['email_validation'] == 'invalid').drop('email_validation')
            email_pass = True

        if ("phone" and "mobilephone" and "homephone") in table_cols and phone_pass == False:
            if email_pass == True:
                temp_valid_df = valid_email_df.withColumn( 'phone_validation', phoneValidateUDF(col("phone"))).withColumn( 'mobilephone_validation', phoneValidateUDF(col("mobilephone"))).withColumn( 'homephone_validation', phoneValidateUDF(col("homephone")))
                temp_invalid_df = invalid_email_df.withColumn( 'phone_validation', phoneValidateUDF(col("phone"))).withColumn( 'mobilephone_validation', phoneValidateUDF(col("mobilephone"))).withColumn( 'homephone_validation', phoneValidateUDF(col("homephone")))
                valid_phone_df = temp_valid_df.filter((temp_valid_df['phone_validation'] == 'valid') | (temp_valid_df['mobilephone_validation'] == 'valid') | (temp_valid_df['homephone_validation'] == 'valid')).drop('phone_validation','mobilephone_validation','homephone_validation')
                invalid_phone_df = temp_invalid_df.filter((temp_invalid_df['phone_validation'] == 'invalid') & (temp_invalid_df['mobilephone_validation'] == 'invalid') & (temp_invalid_df['homephone_validation'] == 'invalid')).drop('phone_validation','mobilephone_validation','homephone_validation')
                phone_pass = True
            else:
                temp_df = no_dupes.withColumn( 'phone_validation', phoneValidateUDF(col("phone"))).withColumn( 'mobilephone_validation', phoneValidateUDF(col("mobilephone"))).withColumn( 'homephone_validation', phoneValidateUDF(col("homephone")))
                valid_phone_df = validation_df.filter((temp_df['phone_validation'] == 'valid') | (temp_df['mobilephone_validation'] == 'valid') | (temp_df['homephone_validation'] == 'valid')).drop('phone_validation','mobilephone_validation','homephone_validation')
                invalid_phone_df = validation_df.filter((temp_df['phone_validation'] == 'invalid') & (temp_df['mobilephone_validation'] == 'invalid') & (temp_df['homephone_validation'] == 'invalid')).drop('phone_validation','mobilephone_validation','homephone_validation')
                phone_pass = True

        if ("phone" and "mobilephone") in table_cols and phone_pass == False:
            if email_pass == True:
                temp_valid_df = valid_email_df.withColumn( 'phone_validation', phoneValidateUDF(col("phone"))).withColumn( 'mobilephone_validation', phoneValidateUDF(col("mobilephone")))
                temp_invalid_df = invalid_email_df.withColumn( 'phone_validation', phoneValidateUDF(col("phone"))).withColumn( 'mobilephone_validation', phoneValidateUDF(col("mobilephone")))
                valid_phone_df = temp_valid_df.filter((temp_valid_df['phone_validation'] == 'valid') | (temp_valid_df['mobilephone_validation'] == 'valid')).drop('phone_validation','mobilephone_validation')
                invalid_phone_df = temp_invalid_df.filter((temp_invalid_df['phone_validation'] == 'invalid') & (temp_invalid_df['mobilephone_validation'] == 'invalid')).drop('phone_validation','mobilephone_validation')
                phone_pass = True
            else:
                temp_df = no_dupes.withColumn( 'phone_validation', phoneValidateUDF(col("phone"))).withColumn( 'mobilephone_validation', phoneValidateUDF(col("mobilephone")))
                valid_phone_df = validation_df.filter((temp_df['phone_validation'] == 'valid') | (temp_df['mobilephone_validation'] == 'valid')).drop('phone_validation','mobilephone_validation')
                invalid_phone_df = validation_df.filter((temp_df['phone_validation'] == 'invalid') & (temp_df['mobilephone_validation'] == 'invalid')).drop('phone_validation','mobilephone_validation')
                phone_pass = True

        if "phone" in table_cols and phone_pass == False:
            if email_pass == True:
                temp_valid_df = valid_email_df.withColumn( 'phone_validation', phoneValidateUDF(col("phone")))
                temp_invalid_df = invalid_email_df.withColumn( 'phone_validation', phoneValidateUDF(col("phone")))
                valid_phone_df = temp_valid_df.filter((temp_valid_df['phone_validation'] == 'valid')).drop('phone_validation')
                invalid_phone_df = temp_invalid_df.filter((temp_invalid_df['phone_validation'] == 'invalid')).drop('phone_validation')
                phone_pass = True
            else:
                temp_df = no_dupes.withColumn( 'phone_validation', phoneValidateUDF(col("phone")))
                valid_phone_df = validation_df.filter((temp_df['phone_validation'] == 'valid')).drop('phone_validation')
                invalid_phone_df = validation_df.filter((temp_df['phone_validation'] == 'invalid')).drop('phone_validation')
                phone_pass = True
 
        # valid df
        if valid_email_df == 0 and valid_phone_df == 0: 
            valid_df = no_dupes # no invalid df
        elif valid_email_df != 0 and valid_phone_df != 0: # both email and phone df's - valid_phone_df is the combination of both
            valid_df = valid_phone_df
        elif valid_email_df == 0 and valid_phone_df != 0:
            valid_df = valid_phone_df
        elif valid_email_df != 0 and valid_phone_df == 0:
            valid_df = valid_email_df 

        # invalid df
        invalid_df = spark.range(0).drop("id") # creates empty df

        if invalid_email_df != 0 and invalid_phone_df != 0:
            invalid_df = invalid_phone_df
        elif invalid_email_df == 0 and invalid_phone_df != 0:
            invalid_df = invalid_phone_df
        elif invalid_email_df != 0 and invalid_phone_df == 0:
            invalid_df = invalid_email_df

        # convert back to dynamicframe
        valid_df_end = DynamicFrame.fromDF(valid_df, glueContext, 'valid_data')
        invalid_df_end = DynamicFrame.fromDF(invalid_df,glueContext, 'invalid_data')

        print("valid df count: ", valid_df_end.count())
        print("invalid df count: ", invalid_df_end.count())

        # repartition dynamicframes to output exactly 20 files to s3
        valid_df_end = valid_df_end.repartition(20)
        invalid_df_end = invalid_df_end.repartition(20)

        
        # write to targets
        quarantine_bucket = glueContext.write_dynamic_frame.from_options(
            frame=invalid_df_end,
            connection_type='s3',
            format='glueparquet',
            connection_options={
                'path': 's3://{}'.format(quarantine_table_folder)
            },
            transformation_ctx ='quarantine_bucket'
        )

        trusted_bucket = glueContext.write_dynamic_frame.from_options(
            frame=valid_df_end,
            connection_type='s3',
            format='glueparquet',
            connection_options={
                'path': 's3://{}'.format(trusted_table_folder)
            },
        )
        
    else:
        print(s3_table_prefix, "does not exists")
        
job.commit()