import boto3
from datetime import date, timedelta

client = boto3.client('glue')
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
source_bucket = 'datalake-raw-510716259290'
tables = ['pgastage_pga_cen_cust_addr','pgastage_pga_cen_cust_cyber','pgastage_pga_cen_cust_mast','pgastage_pga_cen_cust_phone','pgastage_pga_evt_reg_hdr','pgastage_pga_evt_reg_tourn_info']

iterable_tables = ['iterable_api_campaigns','iterable_api_users','iterable_api_email_clicks','iterable_api_email_opens','iterable_api_email_subscribes','iterable_api_email_unsubscribes', 'iterable_api_message_types', 'iterable_api_lists']
prior_date = str(date.today()  - timedelta(days=1) ).replace("-","/")
todays_date =str(date.today()).replace("-","/")
print("prior date: ", prior_date)
print("todays date: ", todays_date)

# iterable api
for t in iterable_tables:
    s3_table_prefix = "{}/{}/{}".format('iterable_api', t, todays_date)
    results = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=s3_table_prefix)
    s3_table_prefix_exists = 'Contents' in results
    if not s3_table_prefix_exists:
        s3_table_prefix = "{}/{}/{}".format('iterable_api', t, prior_date)
        resp = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=s3_table_prefix)
        for obj in resp['Contents']:
    
            copy_source = {
                'Bucket': source_bucket,
                'Key': obj['Key']
            }
    
            updated_key = obj['Key'].replace(prior_date, todays_date)
            bucket = s3_resource.Bucket(source_bucket)
            bucket.copy(copy_source, updated_key)
            
            response = client.start_job_run(JobName = 'rtt-glue-job',
                              Arguments= {'--source_system_name': 'iterable_api',
                                          '--source_system_table_name': t})
            
            print('## GLUE JOB RUN ID: ' + response['JobRunId'])

# ccms
'''
for t in tables:
    s3_table_prefix = "{}/{}/{}".format('ccms', t, prior_date)
    resp = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=s3_table_prefix)
    for obj in resp['Contents']:

        copy_source = {
            'Bucket': source_bucket,
            'Key': obj['Key']
        }

        updated_key = obj['Key'].replace(prior_date, todays_date)
        bucket = s3_resource.Bucket(source_bucket)
        bucket.copy(copy_source, updated_key)
        
# kick off RTT job for CCMS

response = client.start_job_run(JobName = 'rtt-glue-job',
                  Arguments= {'--source_system_name': 'ccms',
                              '--source_database_instance_name': 'pgastage'})

print('## GLUE JOB RUN ID: ' + response['JobRunId'])
'''