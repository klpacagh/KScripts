
# Set up logging
import json
import os
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Import Boto 3 for AWS Glue
import boto3
client = boto3.client('glue')

# Variables for the job: 
glueJobName = os.environ['GLUE_JOB_NAME']

# Define Lambda function
def lambda_handler(event, context):
    logger.info('## INITIATED BY EVENT: ')
    
    temp_event = json.loads(event['Records'][0]['body'])
    
    print(temp_event)
    
    if 'detail' in temp_event.keys(): # Salesforce event
    
        appflow_source = temp_event['detail']['source']
        logger.info(temp_event['detail'])
        if 'salesforce' in appflow_source:
            system_name = (appflow_source.split('/')[1]).lower()
            system_table_name = (temp_event['detail']['source-object']).lower()
            
            print(system_name)
            print(system_table_name)
            response = client.start_job_run(JobName = glueJobName,
                            Arguments= {'--source_system_name': system_name,
                                        '--source_system_table_name': system_table_name})

    else:
        response = client.start_job_run(JobName = glueJobName,
                            Arguments= {'--source_system_name': temp_event['system_name']})
                
                
    logger.info('## STARTED CLEANING GLUE JOB: ' + glueJobName)
    logger.info('## GLUE JOB RUN ID: ' + response['JobRunId'])
    
    
    return response
