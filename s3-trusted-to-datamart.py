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
from pyspark.sql.functions import col, split, lit, regexp_replace, current_timestamp, expr, udf
from pyspark.sql.types import StringType, BooleanType, DoubleType, LongType, DateType, IntegerType
from helpers import get_db_instances, create_table_dict, dropNullTableRecords, addCreatedAndUpdatedAt

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'trusted_bucket', 'refined_bucket', 'config_file_path', 'connection_name', 'redshift_role_arn', 'db_name', 'schema_name'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()
    
todays_date =str(date.today()).replace("-","/")

system_to_db_map = get_db_instances('rtt-glue-job', 200)

data_dict = create_table_dict(glueContext, args["config_file_path"], args["trusted_bucket"], system_to_db_map, todays_date)
        
data_dict['salesforce-career-services_salesforce-career-services-contact'].createOrReplaceTempView('salesforce_contact')

# select columns from salesforce
salesforce = spark.sql("select trim(BOTH ' \t' FROM Email) as Email, Phone, HomePhone, MobilePhone, FirstName, LastName, Birthdate, MailingStreet, MailingCity, MailingState, MailingCountry, MailingPostalCode, Gender__c as Gender from salesforce_contact")

# manipulate dataframe columns to match target schema
salesforce = salesforce.withColumn("Address1", split(col("MailingStreet"), ",").getItem(0)).withColumn("Address2", split(col("MailingStreet"), ",").getItem(1)).drop("MailingStreet")
salesforce = salesforce.withColumn("juniorParent", lit(None)).withColumn("home_course", lit(None)).withColumn("Handicap", lit(None))
salesforce = salesforce.dropDuplicates(['Email','FirstName','LastName']).dropna(subset=['Email'])
print("Salesforce dataframe created")

data_dict['coach_tools_payment_accounts'].createOrReplaceTempView('payment_accounts')
data_dict['coach_tools_contacts'].createOrReplaceTempView('contacts')
data_dict['coach_tools_push_notification_device_tokens'].createOrReplaceTempView('devices')
data_dict['coach_tools_coach_connections'].createOrReplaceTempView('connections')
data_dict['coach_tools_demographic_profiles'].createOrReplaceTempView('profiles')
data_dict['coach_tools_accounts'].createOrReplaceTempView('accounts')
data_dict['coach_tools_representatives'].createOrReplaceTempView('representatives')
data_dict['coach_tools_students'].createOrReplaceTempView('students')

# join coach_tools tables
data_dict['coach_tools_contacts'].createOrReplaceTempView('contacts')
data_dict['coach_tools_coach_connections'].createOrReplaceTempView('connections')
data_dict['coach_tools_demographic_profiles'].createOrReplaceTempView('profiles')
data_dict['coach_tools_representatives'].createOrReplaceTempView('representatives')
data_dict['coach_tools_students'].createOrReplaceTempView('students')

# join coach_tools tables
print("joining coach tables")
coach = spark.sql("select trim(BOTH ' \t' FROM a.email) as email, a.phone_number, a.address1, a.address2, a.city, a.state, a.zip, b.first_name, b.last_name, d.gender, case when e.relationship_type = 'PARENT' then TRUE else FALSE end juniorParent from contacts a left join students b on trim(BOTH ' \t' FROM a.email) = trim(BOTH ' \t' FROM b.email) left join connections c on b.id = c.student_id left join profiles d on c.student_id = d.student_id left join representatives e on a.id = e.contact_id")
# append null columns to match target schema
coach = coach.withColumn('country', lit(None)).withColumn("home_course", lit(None)).withColumn("handicap", lit(None))
coach = coach.dropDuplicates(['email','first_name','last_name']).dropna(subset=['email'])
print("Coach dataframe created")

data_dict['ccms_evt_reg_tourn_info'].createOrReplaceTempView('tournaments')
data_dict['ccms_cen_cust_phone'].createOrReplaceTempView('phone')
data_dict['ccms_cen_cust_cyber'].createOrReplaceTempView('cyber')
data_dict['ccms_cen_cust_addr'].createOrReplaceTempView('address')
data_dict['ccms_cen_cust_mast'].createOrReplaceTempView('master')
data_dict['ccms_evt_reg_hdr'].createOrReplaceTempView('hdr')

# join CCMS tables
print("joining CCMS tables")
ccms = spark.sql("select a.cust_id, a.first_nm, a.last_nm, a.birth_dt,a.sex, b.street1, b.city_nm, b.state_cd, b.country_nm, b.postal_cd,  trim(BOTH ' \t' FROM c.cyber_txt) as cyber_txt, d.phone_num, f.home_course, f.HANDICAP from master a left join address b on a.cust_id = b.cust_id left join cyber c on b.cust_id = c.cust_id left join phone d on c.cust_id = d.cust_id left join hdr e on d.cust_id = e.cust_id left join tournaments f on e.REGI_SERNO = f.REGI_SERNO")
# append null columns to match target schema
ccms = ccms.withColumn("street2", lit(None)).withColumn("juniorParent", lit(None)).withColumn("home_course", lit(None)).withColumn("handicap", lit(None))
ccms = ccms.dropDuplicates(['cyber_txt','first_nm','last_nm']).dropna(subset=['cyber_txt'])
print("CCMS dataframe created")

# union source system tables 
salesforce.createOrReplaceTempView('salesforce')
coach.createOrReplaceTempView('coach')
ccms.createOrReplaceTempView('ccms')

# union source system tables 
print('joining source sytem tables')
joined = spark.sql("(select trim(BOTH '\t' FROM Email) as email, Phone as phoneNumber, FirstName as firstName, LastName as lastName, juniorParent, Gender as genderEst, Address1 as address1, Address2 as address2, MailingCity as city, MailingState as stateRegion, MailingCountry as country, MailingPostalCode as postalCode, home_course as homeCourse, Handicap as handicap from Salesforce) UNION (select trim(BOTH '\t' FROM email), phone_number, first_name, last_name, juniorParent, gender, address1, address2, city, state, country, zip, home_course, handicap from coach) UNION (select trim(BOTH '\t' FROM cyber_txt) as email, phone_num, first_nm, last_nm, juniorParent, sex, street1, street2, city_nm, state_cd, country_nm, postal_cd, home_course, HANDICAP from ccms)")

table_dict = {}

# format phone numbers for iterable-ready ingestion
joined = joined.withColumn("phoneNumber", regexp_replace("phoneNumber", "[^0-9a-zA-Z$]+", "")).withColumn("id", expr("uuid()")).withColumn("profile_id", expr("uuid()")).withColumn("email_id", expr("uuid()"))

joined = joined.cache()
print("joined count: ", joined.count())


# Id needs to be consumers.profile_id
table_dict['emails'] = joined.select(col('email_id').alias('id'),
                       col('email'))

## Id needs to be consumers.email_id
table_dict['profiles'] = joined.select(col('profile_id').alias('id'),
                         col('firstName').alias('name_first'),
                         col('lastName').alias('name_last'), 
                         col('genderEst').alias('gender'),
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
                        col('stateRegion').alias('state_region'),
                        lit(None).cast(StringType()).alias('int_region'),
                        'country',
                        col('postalCode').alias('postal_code')).withColumn('id',expr("uuid()"))

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


for k,v in table_dict.items():
    
    if table_dict[k].rdd.isEmpty():
        print('No data for {}'.format(k))
        continue
    
    datamart_table = DynamicFrame.fromDF(table_dict[k], glueContext, k)

    # make sure target folder is empty as dynamicframes will not overwrite
    try:
        glueContext.purge_s3_path('s3://{}/{}/{}/{}'.format(args['refined_bucket'],'datamart', k, todays_date), {"retentionPeriod": 0})
    except:
        print("Exception occured in clearing target folder location")
        
    print('writing {} to s3'.format(k))
    
    glueContext.write_dynamic_frame.from_options(
            frame=datamart_table,
            connection_type='s3',
            format='json',
            connection_options={
                'path': 's3://{}/{}/{}/{}'.format(args['refined_bucket'],'datamart', k, todays_date)
            },
        )
    
    print('writing {} to Redshift'.format(k))
    
    my_conn_options = {
        'dbtable': '{}.{}'.format(args['schema_name'], k),
        'preactions':'truncate table {}.{}'.format(args['schema_name'],k),
        'database': args['db_name'],
        'aws_iam_role': args['redshift_role_arn']}
    
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=datamart_table,
        catalog_connection=args['connection_name'],
        connection_options=my_conn_options,
        transformation_ctx='write_{}_table'.format(k),
        redshift_tmp_dir= args['TempDir']
    )
