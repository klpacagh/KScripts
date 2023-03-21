import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
import boto3
from datetime import date
from pyspark.sql.functions import col, split, lit, udf, regexp_replace, create_map, array
from pyspark.sql.types import ArrayType, StringType, MapType
import json
import io
from helpers import get_db_instances, create_table_dict, remove_null_fields

todays_date =str(date.today()).replace("-","/")

args = getResolvedOptions(sys.argv, ["JOB_NAME", "trusted_bucket", "refined_bucket","config_file_path"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"] + str(todays_date))

system_to_db_map = get_db_instances('crossaccount-rtt-bus')

data_dict = create_table_dict(glueContext, args["config_file_path"], args["trusted_bucket"], system_to_db_map, todays_date)

data_dict['salesforce-career-services_salesforce-career-services-contact'].createOrReplaceTempView('salesforce_contact')

# select columns from salesforce
print("joining Salesforce tables")
salesforce = spark.sql("select trim(BOTH ' \t' FROM Email) as Email, Phone, HomePhone, MobilePhone, FirstName, LastName, Birthdate, MailingStreet, MailingCity, MailingState, MailingCountry, MailingPostalCode, Gender__c as Gender from salesforce_contact")

# manipulate dataframe columns to match target schema
salesforce = salesforce.withColumn("Address1", split(col("MailingStreet"), ",").getItem(0)).withColumn("Address2", split(col("MailingStreet"), ",").getItem(1)).drop("MailingStreet")
salesforce = salesforce.withColumn("juniorParent", lit(None)).withColumn("home_course", lit(None)).withColumn("Handicap", lit(None))
salesforce = salesforce.dropDuplicates(['Email','FirstName','LastName']).dropna(subset=['Email'])
print("Salesforce dataframe created")

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

print("joining source sytem tables")
joined = spark.sql("(select trim(BOTH '\t' FROM Email) as email, Phone as phoneNumber, FirstName as firstName, LastName as lastName, juniorParent, Gender as genderEst, Address1 as address1, Address2 as address2, MailingCity as city, MailingState as stateRegion, MailingCountry as country, MailingPostalCode as postalCode, home_course as homeCourse, Handicap as handicap from Salesforce) UNION (select trim(BOTH '\t' FROM email), phone_number, first_name, last_name, juniorParent, gender, address1, address2, city, state, country, zip, home_course, handicap from coach) UNION (select trim(BOTH '\t' FROM cyber_txt) as email, phone_num, first_nm, last_nm, juniorParent, sex, street1, street2, city_nm, state_cd, country_nm, postal_cd, home_course, HANDICAP from ccms)")

# format phone numbers for iterable-ready ingestion
print("formatting phone numbers")
joined = joined.withColumn("phoneNumber", regexp_replace("phoneNumber", "[^0-9a-zA-Z$]+", ""))

# create new json objects in memory by removing null fields before writing to bucket
remove_null_fields(joined, args['refined_bucket'], todays_date)
print('{}:TTR Successful'.format(todays_date))

job.commit()