import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import json
import boto3
from datetime import date
import csv
import os
from pyspark.sql.types import StringType,IntegerType
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, split, lit, regexp_replace, current_timestamp, monotonically_increasing_id, expr, udf
from pyspark.sql.types import StringType, BooleanType, DoubleType, LongType, DateType, IntegerType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

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
    
#todays_date =str(date.today()).replace("-","/")
todays_date = '2022/11/21'

glue_client = boto3.client('glue')
glue_job_response_temp = glue_client.get_job_runs(JobName='rtt-glue-job', MaxResults=100)
glue_run_list =  glue_job_response_temp['JobRuns']
# print(glue_run_list)

system_to_db_map = {}
            
for item in glue_run_list:
    if 'Arguments' in item:
        if '--source_database_instance_name' in item['Arguments']:
            if item['Arguments']['--source_system_name'] not in system_to_db_map:
                system_to_db_map[item['Arguments']['--source_system_name']] = item['Arguments']['--source_database_instance_name']

print("system to db map: ", system_to_db_map)

s3_client = boto3.client('s3')
trusted_bucket = "datalake-trusted-510716259290"
refined_bucket = "datalake-refined-510716259290"
config_file_path = "enterprise-data-glue-scripts-510716259290/sttm_config.csv"

configDF = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="csv",
    format_options= {'withHeader': True},
    connection_options={"paths": ["s3://{}".format(config_file_path)], "recurse": True},
    transformation_ctx="config_node_ctx"
)


configDF = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="csv",
    format_options= {'withHeader': True},
    connection_options={"paths": ["s3://{}".format(config_file_path)], "recurse": True},
    transformation_ctx="config_node_ctx"
)

configCollection = configDF.toDF().select('source_system','schema','source_table').distinct().collect()

data_dict = {}

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
        
data_dict['salesforce-career-services_salesforce-career-services-contact'].createOrReplaceTempView('salesforce_contact')

# select columns from salesforce
salesforce = spark.sql("select Email, Phone, HomePhone, MobilePhone, FirstName, LastName, Birthdate, MailingStreet, MailingCity, MailingState, MailingCountry, MailingPostalCode, Gender__c as Gender from salesforce_contact")

# manipulate dataframe columns to match target schema
salesforce = salesforce.withColumn("Address1", split(col("MailingStreet"), ",").getItem(0)).withColumn("Address2", split(col("MailingStreet"), ",").getItem(1)).drop("MailingStreet")
salesforce = salesforce.withColumn("relationship_type", lit(None)).withColumn("home_course", lit(None)).withColumn("Handicap", lit(None))
salesforce = salesforce.dropDuplicates(['Email']).dropna(subset=['Email'])

data_dict['coach_tools_payment_accounts'].createOrReplaceTempView('payment_accounts')
data_dict['coach_tools_contacts'].createOrReplaceTempView('contacts')
data_dict['coach_tools_push_notification_device_tokens'].createOrReplaceTempView('devices')
data_dict['coach_tools_coach_connections'].createOrReplaceTempView('connections')
data_dict['coach_tools_demographic_profiles'].createOrReplaceTempView('profiles')
data_dict['coach_tools_accounts'].createOrReplaceTempView('accounts')
data_dict['coach_tools_representatives'].createOrReplaceTempView('representatives')
data_dict['coach_tools_students'].createOrReplaceTempView('students')

# join coach_tools tables
coach = spark.sql("select a.email, a.phone_number, a.address1, a.address2, a.city, a.state, a.zip, b.first_name, b.last_name, d.gender, e.relationship_type from contacts a left join students b on a.email = b.email left join connections c on b.id = c.student_id left join profiles d on c.student_id = d.student_id left join representatives e on a.id = e.contact_id")

# append null columns to match target schema
coach = coach.withColumn('country', lit(None)).withColumn("home_course", lit(None)).withColumn("handicap", lit(None))
coach = coach.dropDuplicates(['email']).dropna(subset=['email'])

data_dict['ccms_evt_reg_tourn_info'].createOrReplaceTempView('tournaments')
data_dict['ccms_cen_cust_phone'].createOrReplaceTempView('phone')
data_dict['ccms_cen_cust_cyber'].createOrReplaceTempView('cyber')
data_dict['ccms_cen_cust_addr'].createOrReplaceTempView('address')
data_dict['ccms_cen_cust_mast'].createOrReplaceTempView('master')
data_dict['ccms_evt_reg_hdr'].createOrReplaceTempView('hdr')

# join CCMS tables
ccms = spark.sql("select a.cust_id, a.first_nm, a.last_nm, a.birth_dt,a.sex, b.street1, b.street2, b.city_nm, b.state_cd, b.country_nm, b.postal_cd,  c.cyber_txt, d.phone_num, f.home_course, f.HANDICAP from master a left join address b on a.cust_id = b.cust_id left join cyber c on b.cust_id = c.cust_id left join phone d on c.cust_id = d.cust_id left join hdr e on d.cust_id = e.cust_id left join tournaments f on e.REGI_SERNO = f.REGI_SERNO")

ccms = ccms.withColumn("home_course", lit(None)).withColumn("handicap", lit(None))
ccms = ccms.dropDuplicates(['cyber_txt','first_nm','last_nm']).dropna(subset=['cyber_txt'])

# cast all system columns as strings before union
systems = [salesforce, coach, ccms]
for system in systems:
    system = system.select([col('{}'.format(c)).cast(StringType()).alias(c) for c in system.columns])
    
salesforce.createOrReplaceTempView('salesforce')
coach.createOrReplaceTempView('coach')
ccms.createOrReplaceTempView('ccms')

# union source system tables 
print('joining source sytem tables')
joined = spark.sql("(select Email as email, Phone as phoneNumber, FirstName as firstName, LastName as lastName, Gender as gender, Address1 as address1, Address2 as address2, MailingCity as city, MailingState as state, MailingCountry as country, MailingPostalCode as postalCode, home_course as homeCourse, Handicap as handicap from Salesforce) UNION (select email, phone_number, first_name, last_name, gender, address1, address2, city, state, country, zip, home_course, handicap from coach) UNION (select cyber_txt, phone_num, first_nm, last_nm, sex, street1, street2, city_nm, state_cd, country_nm, postal_cd, home_course, HANDICAP from ccms)")

table_dict = {}

# format phone numbers for iterable-ready ingestion
joined = joined.withColumn("phoneNumber", regexp_replace("phoneNumber", "[^0-9a-zA-Z$]+", "")).withColumn("id", expr("uuid()")).withColumn("profile_id", expr("uuid()")).withColumn("email_id", expr("uuid()"))

joined = joined.cache()
joined.count()

# Id needs to be consumers.profile_id
table_dict['emails'] = joined.select(col('email_id').alias('id'),
                       col('email'))

## Id needs to be consumers.email_id
table_dict['profiles'] = joined.select(col('profile_id').alias('id'),
                         col('firstName').alias('name_first'),
                         col('lastName').alias('name_last'), 
                         col('gender'),
                         lit(None).cast(StringType()).alias('league'),
                         lit(None).cast(StringType()).alias('team')) 

## add email_id and phone_id

## id is primary key, foreign key to consumer_interests, consumer_address, orders, sources

table_dict['consumers'] = joined.select(
            col('id'),
            col('profile_id'),
            col('email_id'),
            lit(None).cast(BooleanType()).alias('do_not_sell'),
            lit(None).cast(DoubleType()).alias('total_ticket_spend'),
            lit(None).cast(DoubleType()).alias('total_merch_spend'),
            lit(None).cast(DoubleType()).alias('total_pga_spend'),
            lit(None).cast(BooleanType()).alias('secondary_purchaser'),
            lit(None).cast(BooleanType()).alias('trade_in_purchaser'),
            lit(None).cast(BooleanType()).alias('travel_purchaser'),
            lit(None).cast(DoubleType()).alias('merch_purchase_likely'),
            lit(None).cast(DoubleType()).alias('ticket_purchase_liekly'),
            lit(None).cast(BooleanType()).alias('internal'),
            lit(None).cast(BooleanType()).alias('vip_status'),
            lit(None).cast(BooleanType()).alias('pga_professional_id'),
            col('handicap').cast(LongType()).alias('golf_handicap'),
            lit(None).cast(StringType()).alias('dma_region'),
            col('homeCourse').alias('course_home'),
            lit(None).cast(BooleanType()).alias('has_coach'),
            lit(None).cast(StringType()).alias('volunteer_history'),
            lit(None).cast(DateType()).alias('sign_up_at'),
            lit(None).cast(StringType()).alias('sign_up_source'))

## consumer id is foreign key to consumers.id
## interest id is foreign key to interests.id
table_dict['consumer_interests'] = table_dict['consumers'].select(lit(None).cast(IntegerType()).alias('id'),
                                   col('id').alias('consumer_id'),
                                   lit(None).cast(IntegerType()).alias('interest_id'),
                                   lit(None).cast(BooleanType()).alias('email_opt_in')).withColumn('id',expr("uuid()")).withColumn('interest_id',expr("uuid()"))

## id is primary key and foreign key to consumer_interests.interest_id

table_dict['interests'] = table_dict['consumer_interests'].select(col('interest_id').alias('id'),
                                      lit(None).cast(StringType()).alias('name'),
                                      lit(None).cast(StringType()).alias('type'),
                                      lit(None).cast(IntegerType()).alias('parent'))

table_dict['addresses'] = joined.select(lit(None).cast(IntegerType()).alias('id'),
                        col('address1').alias('address_1'),
                        col('address2').alias('address_2'),
                        lit(None).cast(StringType()).alias('address_3'),
                        lit(None).cast(StringType()).alias('address_4'),
                        'city',
                        col('state').alias('state_region'),
                        lit(None).cast(StringType()).alias('int_region'),
                        'country',
                        'postalCode').withColumn('id',expr("uuid()"))

temp = table_dict['consumers'].select(lit(None).cast(IntegerType()).alias('id'),
                                      col('id').alias('consumer_id'),
                                     lit(None).cast(IntegerType()).alias('address_id')).withColumn('id',expr("uuid()"))
              
    
table_dict['consumer_addresses'] =  temp.join(table_dict['addresses'], temp.address_id == table_dict['addresses'].id, how = 'inner').select([temp.id, temp.consumer_id, table_dict['addresses'].address_1])

table_dict['orders'] = table_dict['consumer_addresses'].select(lit(None).alias('id'),
                            col('consumer_id'),
                            lit(None).cast(IntegerType()).alias('source_id'),
                            lit(None).cast(StringType()).alias('source_order_id'),
                            col('id').alias('address_shipping_id'),
                            col('id').alias('address_billing_id'),
                            lit(None).cast(StringType()).alias('campaign'),
                            lit(None).cast(StringType()).alias('site'),
                            lit(None).cast(DoubleType()).alias('total_discount'),
                            lit(None).cast(DoubleType()).alias('total_gross_sub'),
                            lit(None).cast(DoubleType()).alias('total_taxable_sub'),
                            lit(None).cast(DoubleType()).alias('total_nontaxable_sub'),
                            lit(None).cast(DoubleType()).alias('total_tax'),
                            lit(None).cast(DoubleType()).alias('total_ship')).withColumn('id', expr("uuid()")).withColumn('source_id', expr("uuid()"))

table_dict['sources'] = table_dict['orders'].select(col('source_id').alias('Id'),
                        lit(None).cast(StringType()).alias('name'),
                        lit(None).cast(StringType()).alias('url'),
                        lit(None).cast(StringType()).alias('platform'))

table_dict['order_items'] = table_dict['orders'].select(lit(None).alias('Id'),
                                col('Id').alias('order_id'),
                                lit(None).cast(StringType()).alias('source_product_id'),
                                lit(None).cast(DoubleType()).alias('quantity'),
                                lit(None).cast(DoubleType()).alias('unit_price'),
                                lit(None).cast(DoubleType()).alias('item_discount'),
                                lit(None).cast(StringType()).alias('category'),
                                lit(None).cast(StringType()).alias('subcategory'),
                                lit(None).cast(StringType()).alias('sub_class'),
                                lit(None).cast(StringType()).alias('variation_size'),
                                ).withColumn('id',expr("uuid()"))

table_dict = dropNullTableRecords(table_dict)       
table_dict = addCreatedAndUpdatedAt(table_dict)

# table_dict['addresses'].count()

for k,v in table_dict.items():
    
    if table_dict[k].rdd.isEmpty():
        print('No data for {}'.format(k))
        continue
    
    datamart_table = DynamicFrame.fromDF(table_dict[k], glueContext, k)

    # make sure target folder is empty as dynamicframes will not overwrite
    try:
        glueContext.purge_s3_path('s3://{}/{}/{}/{}'.format(refined_bucket,'datamart', k, todays_date), {"retentionPeriod": 0})
    except:
        print("Exception occured in clearing target folder location")
        
    print('writing {} to s3'.format(k))
    
    glueContext.write_dynamic_frame.from_options(
            frame=datamart_table,
            connection_type='s3',
            format='json',
            connection_options={
                'path': 's3://{}/{}/{}/{}'.format(refined_bucket,'datamart', k, todays_date)
            },
        )
    
    print('writing {} to Redshift'.format(k))
    
    my_conn_options = {
        'dbtable': 'public.{}'.format(k),
        'preactions':'truncate table public.{}'.format(k),
        'database': 'dev',
        'aws_iam_role': 'arn:aws:iam::510716259290:role/AWSRedshiftServiceRoleDatalake'}
    
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=datamart_table,
        catalog_connection='kev-rss',
        connection_options=my_conn_options,
        transformation_ctx='write_{}_table'.format(k),
        redshift_tmp_dir="s3://redshift-temp-510716259290"
    )
