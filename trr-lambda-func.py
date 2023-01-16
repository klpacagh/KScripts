
import json
import boto3
from datetime import date
import csv
import os

# setup logging
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):

    rtt_glue_job_name = 'rtt-glue-job'
    ttr_glue_job_name = os.environ['TTR_JOB_NAME']
    
    # check if trr job ran for the day already
    glue_client = boto3.client('glue')
    trr_glue_job_response = glue_client.get_job_runs(JobName=ttr_glue_job_name, MaxResults=1)
    
    # if no job history - run first time
    if len(trr_glue_job_response['JobRuns']) == 0:
        ttr_main(rtt_glue_job_name,ttr_glue_job_name)
    else:
        check = trr_glue_job_response['JobRuns'][0]
        print(check)
        
        # if job already ran and was successful
        if str(check['StartedOn'].strftime('%Y-%m-%d')) == str(date.today()): 
            if check['JobRunState'] == 'SUCCEEDED':
                return "TRR Already completed for the day"
            else:
                print("Running TTR for current day")
                ttr_main(rtt_glue_job_name,ttr_glue_job_name)
                
        # catch for new runs
        else:
            print("Running TTR Adhoc")
            ttr_main(rtt_glue_job_name,ttr_glue_job_name)

def ttr_main(rtt, ttr):
    
    rtt_glue_job_name = rtt
    ttr_glue_job_name = ttr
    
    logger.info('## TRR Process Initiated: ')
    
    todays_date =str(date.today()).replace("-","/")
    print("checking for todays date: ", todays_date)

    s3_resource = boto3.resource('s3')
    s3_client = boto3.client('s3')
    glue_client = boto3.client('glue')
    
    config_bucket_name = os.environ['CONFIG_BUCKET_NAME']
    trusted_bucket_name = os.environ['TRUSTED_BUCKET_NAME']

    s3_file_name = 'sttm_config.csv'
    s3_object = s3_resource.Object(config_bucket_name, s3_file_name)

    glue_job_response_temp = glue_client.get_job_runs(JobName=rtt_glue_job_name, MaxResults=25)
    glue_run_list =  glue_job_response_temp['JobRuns']
    
    system_to_db_map = {}
    
    for item in glue_run_list:
        if 'Arguments' in item:
            if '--source_database_instance_name' in item['Arguments']:
                if item['Arguments']['--source_system_name'] not in system_to_db_map:
                    system_to_db_map[item['Arguments']['--source_system_name']] = item['Arguments']['--source_database_instance_name']
                        
    print(system_to_db_map)

    # read in config file
    data = s3_object.get()['Body'].read().decode('utf-8').splitlines()
    
    system_tables = [] 
    distinct_glue_jobs = [] # count of distinct system/system_tables
    
    lines = csv.reader(data)
    headers = next(lines)

    # store unique object consisting of [system name, full s3 object name] after parsing config file
    for line in lines:
        if 'salesforce' in line[0]:
            if [line[0],line[2]] not in system_tables:
                system_tables.append([line[0],line[2]])
            if line[2] not in distinct_glue_jobs:
                distinct_glue_jobs.append(line[2])
        else:
            if line[0] in system_to_db_map.keys():
                concat_tab_name = "{}_{}_{}".format(system_to_db_map[line[0]], line[1], line[2])

                if [line[0],concat_tab_name] not in system_tables:
                    system_tables.append([line[0],concat_tab_name])
                if line[0] not in distinct_glue_jobs:
                    distinct_glue_jobs.append(line[0])
    

            
    # determine if files for system exists for today
    exists = []
    for item in system_tables:

        print("checking: ", item)
        s3_table_prefix = "{}/{}/{}".format(item[0], item[1], todays_date)
        results = s3_client.list_objects(Bucket=trusted_bucket_name, Prefix=s3_table_prefix)
        s3_table_prefix_exists = 'Contents' in results
        
        if s3_table_prefix_exists:
            print(item, " exists!")
            exists.append(True)
        else:
            print(item, " does not exists!")
            exists.append(False)
            
            
    # if all exists - check for running RTT glue job
    if all(exists):
        print("all folders exists")
        
        # maxresults is count of length of distinct_glue_jobs - obtains run history in desc order
        glue_job_response = glue_client.get_job_runs(JobName=rtt_glue_job_name, MaxResults=len(distinct_glue_jobs)) 
    
        jobs_done = []
        for check in glue_job_response['JobRuns']:
            # only care about today
            if str(check['StartedOn'].strftime('%Y-%m-%d')) == str(date.today()): 
                
                if check['JobRunState'] != 'SUCCEEDED':
                    jobs_done.append(False)
                else:
                    jobs_done.append(True)
                    
        if all(jobs_done): # start TRR glue job
            print("all prior rtt jobs finished")
            response = glue_client.start_job_run(JobName = ttr_glue_job_name)
                            
            logger.info('## STARTED TRR GLUE JOB: ' + glueJobName)
            logger.info('## GLUE JOB RUN ID: ' + response['JobRunId'])
            
        else:
            print("RECENT RTT GLUE JOBS IN INVALID STATE - PLEASE FIX THEN INVOKE TRR JOB DIRECTLY")
    else:
        # set pause and retry ?
        print("MISSING FOLDERS - CHECK FOR RTT GLUE JOB FAILURES")
