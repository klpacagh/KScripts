import boto3
import json
import io
import csv
import re
from pyspark.sql.functions import lit, current_timestamp, udf, col, trim, upper
from pyspark.sql.types import StringType,StructType, ArrayType


def createConfigDF(config_file_path,glueContext):
    # setup config/lookup datafarme
    configDF = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="csv",
        format_options= {'withHeader': True},
        connection_options={"paths": ["s3://{}".format(config_file_path)], "recurse": True},
        transformation_ctx="config_node_ctx"
    )

    configSparkDF = configDF.toDF()

    return configSparkDF

def createDynamicFrameFromS3(target_folder,glueContext,file_extension='parquet'):
    # create table frame
    rawDF = glueContext.create_dynamic_frame.from_options(
        format_options={},
        connection_type="s3",
        format=file_extension,
        connection_options={
            "paths": ["s3://{}".format(target_folder)],
            "groupSize": "134217728",
            "recurse": True
            },
        transformation_ctx="temp_node_ctx"
    ) 

    return rawDF.toDF()

def writeDynamicFrameToS3(target_folder, output_frame, glueContext, transf_ctx='temp_ctx'):

    # make sure target folder is empty as dynamicframes will not overwrite
    try:
        glueContext.purge_s3_path('s3://{}'.format(target_folder), {"retentionPeriod": 0})
    except:
        print("Exception occured in clearing target folder location")

    # write frame to s3
    glueContext.write_dynamic_frame.from_options(
        frame=output_frame,
        connection_type='s3',
        format='glueparquet',
        connection_options={
            'path': 's3://{}'.format(target_folder),
        },
        transformation_ctx =transf_ctx
    )

def write_df_to_redshift(input_df, endpoint, database, schema, dbtable, username, password, iam_role_arn, tempdir):
    
    print(f"writing to Redshift table {dbtable}")
    
    try:

        input_df.write.format("com.databricks.spark.redshift").option("url", f"jdbc:redshift://{endpoint}:5439/{database}?user={username}&password={password}").option("dbtable",f"{schema}.{dbtable}").option("aws_iam_role", iam_role_arn).option("tempdir",tempdir).option("preactions","truncate table %s").option("extracopyoptions","EMPTYASNULL").mode("append").saveAsTable(dbtable)

        print(f"Done writing {dbtable} to Redshift")
    
    except Exception as e:
        print(f"failed to write with exception {e}")

def write_df_to_s3(input_df, table, s3_target, glueContext):
        
    try:
        glueContext.purge_s3_path(s3_target, {"retentionPeriod": 0})
    except:
        print("Exception occured in clearing target folder location")
        
    print('writing {} to s3'.format(table))
    
    try:
        input_df.write.format("org.apache.spark.sql.json").mode("overwrite").save(s3_target)
    except Exception as e:
        print(f"Failed to write to S3 with exception {e}")

def write_to_ttdm_targets(glueContext, table_dict, refined_bucket, endpoint, database, schema, username, password, iam_role_arn, tempdir, date, redshift=True,s3=True):
    
    for k in table_dict.keys():
    
        if s3:
            if table_dict[k].rdd.isEmpty():
                print('No data for {}'.format(k))
                continue
            
            refined_table_folder = "s3://{}/{}/{}/{}".format(refined_bucket, "datamart", k, date)
    
            write_df_to_s3(table_dict[k], k, refined_table_folder, glueContext)
        
        if redshift:
            write_df_to_redshift(table_dict[k], endpoint, database, schema, k, username, password, iam_role_arn, tempdir)

def get_db_instances(event_bus='crossaccount-rtt-bus'):

    events = boto3.client('events')
    system_to_db_map = {}
    cross_account_event_bus=event_bus
    rules = events.list_rules(EventBusName=cross_account_event_bus)

    for rule in rules['Rules']:
        targets=events.list_targets_by_rule(
        Rule=rule['Name'],
        EventBusName=cross_account_event_bus
        )
        for target in targets['Targets']:
            loaded = json.loads(target['Input'])
            system_name = loaded['jobName'].split('-glue-job')[0]
            db_name = loaded['dbInstance']
            system_to_db_map[system_name] = db_name

    print("SYSTEM_TO_DB_MAP: ", system_to_db_map)

    return system_to_db_map

def create_table_dict(glueContext, config_file_path, trusted_bucket, system_to_db_map, todays_date):

    s3_client = boto3.client('s3')
    data_dict = {}

    configDF = createConfigDF(config_file_path,glueContext)

    configCollection = configDF.select('source_system','schema','source_table').distinct().collect()

    for row in configCollection:

        if row['schema'] == 'null':
            s3_table_prefix = "{}/{}/{}".format(row['source_system'], row['source_table'], todays_date)
        else:
            concat_tab_name = "{}_{}_{}".format(system_to_db_map[row['source_system']], row['schema'], row['source_table'])
            s3_table_prefix = "{}/{}/{}".format(row['source_system'], concat_tab_name, todays_date)
            
         
        latest_s3_table_prefix = FindLatestPrefix(s3_client, trusted_bucket, s3_table_prefix)
        results = s3_client.list_objects_v2(Bucket=trusted_bucket, Prefix=latest_s3_table_prefix)

        s3_table_prefix_exists = 'Contents' in results
    
        if s3_table_prefix_exists:

            print("processing for: ", row['source_system'], ":", row['source_table'])
            trusted_table_folder = "{}/{}".format(trusted_bucket, latest_s3_table_prefix)

            tempDF = createDynamicFrameFromS3(trusted_table_folder,glueContext,file_extension='parquet')
        
            data_dict['{}_{}'.format(row['source_system'], row['source_table'])] = tempDF

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
        if line[0] in line[2]:
            if line[2] not in tables_list:
                tables_list.append(line[2])
                print('{} appended to tables list'.format(line[2]))
            if line[0] not in systems_list:
                systems_list.append(line[0])
                print('{} appended to systems_list'.format(line[0]))
        else:
            if line[0] not in systems_list:
                systems_list.append(line[0])
                print('{} appended to systems_list'.format(line[0]))

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

    print('tables list: ', tables_list)
    print('systems list: ', systems_list)        

    if system_name in systems_list:
        if source_database_instance_name in db_list:
            print('Valid {} input. Continuing...'.format(system_name))
        elif source_system_table_name in tables_list:
            print('Valid {} input. Continuing...'.format(source_system_table_name))
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

def FindLatestPrefix(s3_client, source_bucket, s3_table_prefix):
    results = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=f'{s3_table_prefix}/', Delimiter="/")
    common_prefixes = results.get("CommonPrefixes")

    if common_prefixes is None:
        # Not Appflow
        return s3_table_prefix
    
    # Appflow: get all prefixes
    prefixes = [prefix["Prefix"] for prefix in common_prefixes]
    timestamps = sorted([prefix.split("T")[-1] for prefix in prefixes], reverse=True)
    for prefix in prefixes:
        if prefix.endswith(timestamps[0]):
            return prefix
        
    raise Exception("No prefix found")

def RemExtraSpacesBetween(input):
    if input is not None and type(input) == str:
        return " ".join(input.split())
    return input

def ApplyRemExtraSpacesBetween(df, col_target, cols_are_upper):
    remExSpaceBtwnUDF = udf(lambda x: RemExtraSpacesBetween(x), StringType())

    # split if using dot notation
    try:
        temp_col = col_target.split('.')[-1]
    except:
        temp_col = col_target

    if cols_are_upper:
        temp_col = temp_col.upper()

    temp = df.withColumn(col_target, remExSpaceBtwnUDF(col(col_target))).withColumnRenamed(col_target, temp_col)

    return temp

def TrimFields(input_df, table_cols):

    struct_col_to_rem = set()
    col_types = dict(input_df.dtypes)
    col_types =  {k.lower(): v for k, v in col_types.items()} # lowercase to avoid key error

    try:
        cols_are_upper = input_df.columns[0].isupper()
    except:
        cols_are_upper = False

    print("Trimming cols: ", table_cols)
    for i in table_cols:
        # right conditional gets col data type, skipping timestamp col types
        if i != 'null' and col_types[i.split('.')[0].lower()].split('<')[0] not in ['array','timestamp']:
            print("trimming: ", i)
            input_df = ApplyRemExtraSpacesBetween(input_df,i, cols_are_upper)

            if len(i.split('.')) > 1:
                struct_col_to_rem.add(i.split('.')[0])
        
    # remove struct cols if exist
    input_df = input_df.drop(*list(struct_col_to_rem))

    return input_df


def flatten(schema, prefix=None):
    fields = []
    for field in schema.fields:
        name = prefix + '.' + field.name if prefix else field.name
        dtype = field.dataType
        if isinstance(dtype, ArrayType):
            dtype = dtype.elementType

        if isinstance(dtype, StructType):
            fields += flatten(dtype, prefix=name)
        else:
            fields.append(name.lower())

    return fields

def CheckColumnsExist(input_df, table_cols):
    df_cols_flatten = flatten(input_df.schema)
    for tc in table_cols:
        if tc.lower() not in df_cols_flatten:
            print("Column: ", tc, " does not exist - Adding - Verify if Schema Change Occured in Source")
            input_df = input_df.withColumn(tc, lit(None).cast(StringType()))
            
    return input_df

def removeDuplicateColNames(input_df):
    # scan through columns, throw into set, if found in set, add _dup, then I create new DF with the unique set of columns
    cols_new = [] 
    seen = set()
    for c in input_df.columns:
        if c.lower() not in seen:
            cols_new.append(c)
        else:
            cols_new.append('{}_dup'.format(c))
        
        seen.add(c.lower())
    
    print("removed duplicate column names from dataframe if exists")

    return input_df.toDF(*cols_new).select(*[c for c in cols_new if not c.endswith('_dup')])

def RTTVerifyPhoneAndEmail(cleaned_df, table_cols, system_name, spark_context):

    # initialize UDFs 
    emailValidateUDF = udf(lambda x: ValidateEmail(x), StringType())
    phoneValidateUDF = udf(lambda x: ValidatePhone(x), StringType())

    # empty 
    valid_email_df = 0
    invalid_email_df = 0
    valid_phone_df = 0
    invalid_phone_df = 0
    email_pass = False
    phone_pass = False
    
    indicies = [i for i, s in enumerate(table_cols) if 'email' in s or 'cyber_txt' in s]
    if len(indicies) > 0:
        temp_email_col_name = table_cols[indicies[0]]

        # split if using dot notation
        try:
            email_col_name = temp_email_col_name.split('.')[1]
        except:
            email_col_name = temp_email_col_name
        
        validation_df = cleaned_df.withColumn('email_validation', emailValidateUDF(col(email_col_name)))
        valid_email_df = validation_df.filter(validation_df['email_validation'] == 'valid').drop('email_validation')
        invalid_email_df = validation_df.filter(validation_df['email_validation'] == 'invalid').drop('email_validation')
        email_pass = True

    if ("phone" and "mobilephone" and "homephone") in table_cols and phone_pass == False:
        if email_pass == True:
            temp_valid_df = valid_email_df.withColumn( 'phone_validation', phoneValidateUDF(col("phone"))).withColumn( 'mobilephone_validation', phoneValidateUDF(col("mobilephone"))).withColumn( 'homephone_validation', phoneValidateUDF(col("homephone")))
            valid_phone_df = temp_valid_df.filter((temp_valid_df['phone_validation'] == 'valid') | (temp_valid_df['mobilephone_validation'] == 'valid') | (temp_valid_df['homephone_validation'] == 'valid')).drop('phone_validation','mobilephone_validation','homephone_validation')
            invalid_phone_df = temp_valid_df.filter((temp_valid_df['phone_validation'] == 'invalid') & (temp_valid_df['mobilephone_validation'] == 'invalid') & (temp_valid_df['homephone_validation'] == 'invalid')).drop('phone_validation','mobilephone_validation','homephone_validation')
            phone_pass = True
        else:
            temp_df = cleaned_df.withColumn( 'phone_validation', phoneValidateUDF(col("phone"))).withColumn( 'mobilephone_validation', phoneValidateUDF(col("mobilephone"))).withColumn( 'homephone_validation', phoneValidateUDF(col("homephone")))
            valid_phone_df = validation_df.filter((temp_df['phone_validation'] == 'valid') | (temp_df['mobilephone_validation'] == 'valid') | (temp_df['homephone_validation'] == 'valid')).drop('phone_validation','mobilephone_validation','homephone_validation')
            invalid_phone_df = validation_df.filter((temp_df['phone_validation'] == 'invalid') & (temp_df['mobilephone_validation'] == 'invalid') & (temp_df['homephone_validation'] == 'invalid')).drop('phone_validation','mobilephone_validation','homephone_validation')
            phone_pass = True

    if ("phone" and "mobilephone") in table_cols and phone_pass == False:
        if email_pass == True:
            temp_valid_df = valid_email_df.withColumn( 'phone_validation', phoneValidateUDF(col("phone"))).withColumn( 'mobilephone_validation', phoneValidateUDF(col("mobilephone")))
            valid_phone_df = temp_valid_df.filter((temp_valid_df['phone_validation'] == 'valid') | (temp_valid_df['mobilephone_validation'] == 'valid')).drop('phone_validation','mobilephone_validation')
            invalid_phone_df = temp_valid_df.filter((temp_valid_df['phone_validation'] == 'invalid') & (temp_valid_df['mobilephone_validation'] == 'invalid')).drop('phone_validation','mobilephone_validation')
            phone_pass = True
        else:
            temp_df = cleaned_df.withColumn( 'phone_validation', phoneValidateUDF(col("phone"))).withColumn( 'mobilephone_validation', phoneValidateUDF(col("mobilephone")))
            valid_phone_df = validation_df.filter((temp_df['phone_validation'] == 'valid') | (temp_df['mobilephone_validation'] == 'valid')).drop('phone_validation','mobilephone_validation')
            invalid_phone_df = validation_df.filter((temp_df['phone_validation'] == 'invalid') & (temp_df['mobilephone_validation'] == 'invalid')).drop('phone_validation','mobilephone_validation')
            phone_pass = True

    if ("phone" in table_cols or "phone_number" in table_cols) and phone_pass == False:
        phone_col_name = "phone"
        if "phone_number" in table_cols:
            phone_col_name = "phone_number"

        if email_pass == True:
            temp_valid_df = valid_email_df.withColumn( 'phone_validation', phoneValidateUDF(col(phone_col_name)))
            valid_phone_df = temp_valid_df.filter((temp_valid_df['phone_validation'] == 'valid')).drop('phone_validation')
            invalid_phone_df = temp_valid_df.filter((temp_valid_df['phone_validation'] == 'invalid')).drop('phone_validation')
            phone_pass = True
        else:
            temp_df = cleaned_df.withColumn( 'phone_validation', phoneValidateUDF(col(phone_col_name)))
            valid_phone_df = validation_df.filter((temp_df['phone_validation'] == 'valid')).drop('phone_validation')
            invalid_phone_df = validation_df.filter((temp_df['phone_validation'] == 'invalid')).drop('phone_validation')
            phone_pass = True

    # valid df
    if valid_email_df == 0 and valid_phone_df == 0: 
        valid_df = cleaned_df # no invalid df
    elif valid_email_df != 0 and valid_phone_df != 0: # both email and phone df's - valid_phone_df is the combination of both
        valid_df = valid_phone_df
    elif valid_email_df == 0 and valid_phone_df != 0:
        valid_df = valid_phone_df
    elif valid_email_df != 0 and valid_phone_df == 0:
        valid_df = valid_email_df 

    # invalid df
    invalid_df = spark_context.range(0).drop("id") # creates empty df

    if invalid_email_df != 0 and invalid_phone_df != 0:
        invalid_df = invalid_email_df.union(invalid_phone_df)
    elif invalid_email_df == 0 and invalid_phone_df != 0:
        invalid_df = invalid_phone_df
    elif invalid_email_df != 0 and invalid_phone_df == 0:
        invalid_df = invalid_email_df

    print("valid df count: ", valid_df.count())
    print("invalid df count: ", invalid_df.count())

    return valid_df, invalid_df