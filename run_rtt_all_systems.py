import os
import logging
import csv
import boto3
import json

log = logging.getLogger(__name__)
log_level = os.environ.get('LOG_LEVEL')
if log_level:
    log.setLevel(log_level)

# Import Boto 3 for AWS Glue
client = boto3.client('glue')

# Variables for the job: 
glueJobName = 'rtt-glue-job'


def lambda_handler(event, context):
    
    # ccms
    arguments={'--source_system_name': 'ccms','--source_database_instance_name': 'pgastage'}
    start_glue_job(client=client, arguments=arguments, job_name=glueJobName)                

    # coach_tools
    arguments={'--source_system_name': 'coach_tools','--source_database_instance_name': 'coach_tools'}
    start_glue_job(client=client, arguments=arguments, job_name=glueJobName)     
    
    # axs
    arguments={'--source_system_name': 'axs','--source_database_instance_name': 'elv_pgaamerica'}
    start_glue_job(client=client, arguments=arguments, job_name=glueJobName)    
    
    # sf cs
    arguments= {'--source_system_name': 'salesforce-career-services','--source_system_table_name': 'salesforce-career-services-contact'}
    start_glue_job(client=client, arguments=arguments, job_name=glueJobName) 
    
    # splash that
    arguments= {'--source_system_name': 'splash_that','--source_system_table_name': 'splash_that_group_contacts'}
    start_glue_job(client=client, arguments=arguments, job_name=glueJobName) 
    
    arguments= {'--source_system_name': 'splash_that','--source_system_table_name': 'splash_that_events'}
    start_glue_job(client=client, arguments=arguments, job_name=glueJobName) 
    
    arguments= {'--source_system_name': 'splash_that','--source_system_table_name': 'splash_that_event_details'}
    start_glue_job(client=client, arguments=arguments, job_name=glueJobName) 
    
    # golf genius
    arguments= {'--source_system_name': 'golf_genius','--source_system_table_name': 'golf_genius_master_roster'}
    start_glue_job(client=client, arguments=arguments, job_name=glueJobName)     
 
    # iterable api
    arguments= {'--source_system_name': 'iterable_api','--source_system_table_name': 'iterable_api_email_opens'}
    start_glue_job(client=client, arguments=arguments, job_name=glueJobName)    
    
    arguments= {'--source_system_name': 'iterable_api','--source_system_table_name': 'iterable_api_email_clicks'}
    start_glue_job(client=client, arguments=arguments, job_name=glueJobName)   
    
    arguments= {'--source_system_name': 'iterable_api','--source_system_table_name': 'iterable_api_email_subscribes'}
    start_glue_job(client=client, arguments=arguments, job_name=glueJobName)    
    
    arguments= {'--source_system_name': 'iterable_api','--source_system_table_name': 'iterable_api_email_unsubscribes'}
    start_glue_job(client=client, arguments=arguments, job_name=glueJobName)   
    
    arguments= {'--source_system_name': 'iterable_api','--source_system_table_name': 'iterable_api_users'}
    start_glue_job(client=client, arguments=arguments, job_name=glueJobName)    
    
    arguments= {'--source_system_name': 'iterable_api','--source_system_table_name': 'iterable_api_lists'}
    start_glue_job(client=client, arguments=arguments, job_name=glueJobName)   
    
    arguments= {'--source_system_name': 'iterable_api','--source_system_table_name': 'iterable_api_message_types'}
    start_glue_job(client=client, arguments=arguments, job_name=glueJobName)    
    
    arguments= {'--source_system_name': 'iterable_api','--source_system_table_name': 'iterable_api_campaigns'}
    start_glue_job(client=client, arguments=arguments, job_name=glueJobName) 
    
    return 0
    

def start_glue_job(*, client, arguments, job_name):
    response = client.start_job_run(JobName=job_name,Arguments=arguments)
    log.info('## STARTED JOB: ' + job_name)
    log.info('## GLUE JOB RUN ID: ' + response['JobRunId'])