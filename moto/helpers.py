import boto3
import json
import io
import csv
import re
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from datetime import date
from pyspark.sql.functions import col, split, lit, udf, regexp_replace, create_map, array, current_timestamp
from pyspark.sql.types import ArrayType, StringType, MapType
# from helpers import get_db_instances, create_table_dict, remove_null_fields

def docker_test():
    todays_date =str(date.today()).replace("-","/")

    # args = getResolvedOptions(sys.argv, ["JOB_NAME", "trusted_bucket", "refined_bucket","config_file_path"])
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    print(spark)
    # job = Job(glueContext)
    # job.init(args["JOB_NAME"] + str(todays_date))

    s3_client = boto3.client('s3')
    # data_dict = {}
    config_file_path = "s3://enterprise-data-glue-scripts-510716259290/sttm_config.csv"
    objects = s3_client.list_objects_v2(Bucket="enterprise-data-glue-scripts-510716259290").get("Contents")
    print(objects)

    location = s3_client.get_bucket_location(Bucket="enterprise-data-glue-scripts-510716259290")
    # url = "https://s3-%s.amazonaws.com/%s/%s" % (location, "enterprise-data-glue-scripts-510716259290", "sttm_config.csv")
    print("location: ", location)
    # resp = s3_client.get_object(Bucket="enterprise-data-glue-scripts-510716259290", Key=objects[0]['Key'])
    # file_content = resp["Body"].read().decode('UTF-8')
    # print(file_content)  
    configDF = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="csv",
        format_options= {'withHeader': True},
        connection_options={"paths": ["{}".format(config_file_path)], "recurse": True},
        transformation_ctx="config_node_ctx"
    )
    print(configDF.count())
    # configCollection = configDF.toDF().select('source_system','schema','source_table').distinct().collect()
    # print(configCollection)

def get_db_instances(glueJobName, MaxResults):
    glue_client = boto3.client('glue')
    glue_job_response_temp = glue_client.get_job_runs(JobName=glueJobName, MaxResults=MaxResults)
    glue_run_list =  glue_job_response_temp['JobRuns']

    system_to_db_map = {}
            
    for item in glue_run_list:
        if 'Arguments' in item:
            if '--source_database_instance_name' in item['Arguments']:
                if item['Arguments']['--source_system_name'] not in system_to_db_map:
                    system_to_db_map[item['Arguments']['--source_system_name']] = item['Arguments']['--source_database_instance_name']

    print("system to db map: ", system_to_db_map)
    return system_to_db_map

def create_table_dict(glueContext, config_file_path, trusted_bucket, system_to_db_map, todays_date):

    s3_client = boto3.client('s3')
    data_dict = {}

    configDF = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="csv",
        format_options= {'withHeader': True},
        connection_options={"paths": ["s3://{}".format(config_file_path)], "recurse": True},
        transformation_ctx="config_node_ctx"
    )

    configCollection = configDF.toDF().select('source_system','schema','source_table').distinct().collect()

    for row in configCollection:
    
        if "salesforce" in row['source_system']:
            s3_table_prefix = "{}/{}/{}".format(row['source_system'], row['source_table'], todays_date)
        
        else:
            concat_tab_name = "{}_{}_{}".format(system_to_db_map[row['source_system']], row['schema'], row['source_table'])
            s3_table_prefix = "{}/{}/{}".format(row['source_system'], concat_tab_name, todays_date)
        
        results = s3_client.list_objects(Bucket=trusted_bucket, Prefix=s3_table_prefix)

        s3_table_prefix_exists = 'Contents' in results
    
        if s3_table_prefix_exists:

            print("processing for: ", row['source_system'], ":", row['source_table'])
            trusted_table_folder = "{}/{}".format(trusted_bucket, s3_table_prefix)

            trustedDF = glueContext.create_dynamic_frame.from_options(
                format_options={},
                connection_type="s3",
                format="parquet",
                connection_options={"paths": ["s3://{}".format(trusted_table_folder)], "recurse": True},
                transformation_ctx="temp_node_ctx"
            )
        
            data_dict['{}_{}'.format(row['source_system'], row['source_table'])] = trustedDF.toDF()

    return data_dict

# create new json objects in memory by removing null fields before writing to bucket
def remove_null_fields(joined, refined_bucket, todays_date):
    temp_pan_df = joined.toPandas()
    max_count = joined.count()
    output = io.StringIO()
    file_count = 0
    s3 = boto3.resource('s3')
    data = {}

    for i, j in temp_pan_df.iterrows():
    
        for value in j.keys():
            if j[value] != None and j[value] != "null":
                data[value] = j[value]

        if len(data.keys()) > 1:
            output.write(json.dumps(data))
            output.write("\n")
            # write per 10000 records
            if i % 10000 == 0: 
                contents = output.getvalue()
                key = "{}/{}/iterable_file_{}.json".format('iterable', todays_date, file_count)
                print("writing now to: ", key)
                s3object = s3.Object(refined_bucket, key)

                s3object.put(
                    Body=contents
                )
                output.truncate(0)
                output.seek(0)
                file_count += 1

            elif i == max_count - 1:
                contents = output.getvalue()
        
                key = "{}/{}/iterable_file_{}.json".format('iterable', todays_date, file_count)
                print("writing last file now to: ", key)
                s3object = s3.Object(refined_bucket, key)

                s3object.put(
                    Body=contents
                ) 

            else:
                continue

    output.close()
    
# drop records where every column, except for id columns, is null
def dropNullTableRecords(df_dict):
    subs = []
    return_df_dict = {}
    
    for k,df in df_dict.items():
        
        subs = [c for c in df.columns if "id" not in c.lower()]
        return_df_dict[k] = df.dropna(subset=subs, how='all') 
    return return_df_dict
    
# append created_at and updated_at timestamps to dataframes
def addCreatedAndUpdatedAt(df_dict):
    return_df_dict = {}
    
    for k,df in df_dict.items():
        return_df_dict[k] = df.withColumn('created_at',lit(current_timestamp())).withColumn('updated_at',lit(current_timestamp()))
    return return_df_dict

# assert user inputs are valid
def check_inputs(config_bucket_name, s3_config_file_name, cross_account_event_bus, system_name, source_system_table_name, source_database_instance_name, job_name):

    s3_resource = boto3.resource('s3')
    s3_object = s3_resource.Object(config_bucket_name, s3_config_file_name)

    config_data = s3_object.get()['Body'].read().decode('utf-8').splitlines()

    events = boto3.client('events')
    glue = boto3.client('glue')

    systems_list, tables_list, db_list = [], [], []
    loaded = {}

    lines = csv.reader(config_data)
    next(lines)

    for line in lines:
        if line[0] not in systems_list:
            systems_list.append(line[0])
            if 'salesforce' in line[0] and line[2] not in tables_list:
                tables_list.append(line[2])

    rules = events.list_rules(
                EventBusName= cross_account_event_bus
            )
            
    for rule in rules['Rules']:
        targets=events.list_targets_by_rule(
        Rule=rule['Name'],
        EventBusName=cross_account_event_bus
        )
        for target in targets['Targets']:
            loaded = json.loads(target['Input'])
            db_list.append(loaded['dbInstance'])

    if system_name in systems_list:
        if any('salesforce' in s for s in systems_list) and source_system_table_name in tables_list:
            print('Valid Salesforce input. Continuing...')
        elif source_database_instance_name in db_list:
            print('Valid {} input. Continuing...'.format(system_name))
    else:
        glue.batch_stop_job_run(
            JobName=job_name
        )

#validate phone numbers are 10-13 digits long and only contain numbers
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
    else:
        return 'invalid'

def ValidateEmail(email):
    top_level_domains = ["com","org","net","int","edu","gov","mil","biz"]
    if email is None:
        return "invalid"
        
    em = email.lower()
    # matches email string requirements based on RFC5322 standards
    main_match = re.search(r'[\w.-]+@[\w.-]+.\w+', str(em))

    if main_match:
    
    # search for . after @ and verify top level domain 
        try:
            if em.count("@") > 1:
                return "invalid"
            index_of_amperstand = em.rindex("@") + 1
            local_address = em[:index_of_amperstand - 1]
            if any(x in local_address for x in ['www','http']):
                return "invalid"
            index_of_period = em.rindex(".", index_of_amperstand) + 1
            domain_sub = em[index_of_period:] # matches the domain
            if domain_sub not in top_level_domains:
                return "invalid"
            return "valid"
        except:
            return "invalid"
    return "invalid"
