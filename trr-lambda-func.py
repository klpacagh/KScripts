import os
import logging
import csv
import boto3
import json
from datetime import date,datetime
from botocore.exceptions import ClientError

# Set up logging

log = logging.getLogger(__name__)
log_level = os.environ.get('LOG_LEVEL')
if log_level:
    log.setLevel(log_level)

def paginate_through_jobruns(job_name,max_items=None,page_size=None, starting_token=None):
    glue_client = boto3.client('glue')
    try:
        paginator = glue_client.get_paginator('get_job_runs')
        response = paginator.paginate(JobName=job_name, PaginationConfig={
             'MaxItems':max_items,
             'PageSize':page_size,
             'StartingToken':starting_token}
        )
        return response
    except ClientError as e:
        raise Exception("boto3 client error in paginate_through_jobruns: " + e.__str__())
    except Exception as e:
        raise Exception("Unexpected error in paginate_through_jobruns: " + e.__str__())

def get_failed_jobs(job_run_history):
    failed_jobs = []
    for jobs in job_run_history: # one layer
        # iterate through the jobs
        for job in jobs['JobRuns']:
            if job['StartedOn'].date() == datetime.today().date():
                if job['JobRunState'] not in [ 'SUCCEEDED', 'RUNNING']:
                    if '--source_system_table_name' in job['Arguments'].keys():
                        failed_jobs.append([job['Arguments']['--source_system_name'],job['Arguments']['--source_system_table_name']])
                    else:
                        failed_jobs.append([job['Arguments']['--source_system_name'],'None'])
    return failed_jobs

def lambda_handler(event, context):
    rtt_glue_job_name = os.environ['RTT_JOB_NAME']
    ttr_glue_job_name = os.environ['TTR_JOB_NAME']

    #check if trr job ran for the day already
    glue_client = boto3.client('glue')
    trr_glue_job_response = glue_client.get_job_runs(JobName=ttr_glue_job_name, MaxResults=1)

    # if no job history - run first time
    if len(trr_glue_job_response['JobRuns']) == 0:
        ttr_main(rtt_glue_job_name,ttr_glue_job_name)
    else:
        check = trr_glue_job_response['JobRuns'][0]
        log.info(f'most recent run: {check}')

        # if job already ran and was successful
        if str(check['StartedOn'].strftime('%Y-%m-%d')) == str(date.today()): 
            if check['JobRunState'] in ['SUCCEEDED','RUNNING']:
                log.info('TTR Already completed or is Running for the day')
                return
            else:
                log.info("Running TTR subsequent attempt")
                ttr_main(rtt_glue_job_name,ttr_glue_job_name)
        else:
            # catch for new runs
            log.info("Running TTR First attempt")
            ttr_main(rtt_glue_job_name,ttr_glue_job_name)


def ttr_main(rtt, ttr):
    rtt_glue_job_name = rtt
    ttr_glue_job_name = ttr
    ttdm_glue_job_name =  os.environ['TTDM_JOB_NAME']
    log.info('## TTR Process Initiated')

    todays_date =str(date.today()).replace("-","/")
    
    log.info(f'check for todays date: {todays_date}')

    s3_resource = boto3.resource('s3')
    s3_client = boto3.client('s3')
    glue_client = boto3.client('glue')
    events = boto3.client('events')

    config_bucket_name = os.environ['CONFIG_BUCKET_NAME']
    trusted_bucket_name = os.environ['TRUSTED_BUCKET_NAME']

    s3_file_name = 'sttm_config.csv'
    s3_object = s3_resource.Object(config_bucket_name, s3_file_name)

    system_to_db_map = {}
    cross_account_event_bus=os.environ['CROSSACCOUNT_EVENT_BUS_NAME']
    rules = events.list_rules(EventBusName= cross_account_event_bus)

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

    log.info(f'## SYSTEM_TO_DB_MAP: {system_to_db_map}')

    # read in config file
    data = s3_object.get()['Body'].read().decode('utf-8').splitlines()

    system_tables = [] 

    lines = csv.reader(data)
    headers = next(lines)

    # store unique object consisting of [system name, full s3 object name] after parsing config file
    for line in lines:
        if 'null' in line[1]: # non-glue systems
            if [line[0],line[2]] not in system_tables:
                system_tables.append([line[0],line[2]])
        else:
            if line[0] in system_to_db_map.keys():
                concat_tab_name = "{}_{}_{}".format(system_to_db_map[line[0]], line[1], line[2])
                if [line[0],concat_tab_name] not in system_tables:
                    system_tables.append([line[0],concat_tab_name])

    log.info(f'## SYSTEM TABLES: {system_tables}')
    
    # determine if files for system exists for today
    exists = []
    missing_folders = []
    for item in system_tables:
        log.debug(f'checking: {item}')
        s3_table_prefix = "{}/{}/{}".format(item[0], item[1], todays_date)
        results = s3_client.list_objects(Bucket=trusted_bucket_name, Prefix=s3_table_prefix)
        s3_table_prefix_exists = 'Contents' in results

        if s3_table_prefix_exists:
            log.debug(f'{item} exists!')
            exists.append(True)
        else:
            log.warn(f'{item} does not exists!')
            missing_folders.append(item)
            exists.append(False)
    
    # if all exists - check for running RTT glue job
    if all(exists):
        log.debug("all folders exists")

        # ensure no current running rtt job
        glue_job_response = glue_client.get_job_runs(JobName=rtt_glue_job_name, MaxResults=1) 

        jobs_done = []
        for check in glue_job_response['JobRuns']:
            # only care about today
            if str(check['StartedOn'].strftime('%Y-%m-%d')) == str(date.today()): 
                if check['JobRunState'] != 'SUCCEEDED':
                    jobs_done.append(False)
                else:
                    jobs_done.append(True)
                
        if all(jobs_done): # start TRR glue job
            log.info("all prior rtt jobs finished")
            ttr_job_list = [ttr_glue_job_name,ttdm_glue_job_name]
    
            for ttrjob in ttr_job_list:
                response = glue_client.start_job_run(JobName = ttrjob)
                            
                log.info(f'## STARTED TTR GLUE JOB: {ttrjob}')
                log.info('## GLUE JOB RUN ID: ' + response['JobRunId'])
        else:
            log.warn("RECENT RTT GLUE JOBS IN INVALID STATE - PLEASE FIX THEN INVOKE TRR JOB DIRECTLY")
    else:

        log.warn("MISSING FOLDERS FOR THE FOLLOWING - CHECK FOR RTT GLUE JOB FAILURES")
        log.warn(f'{missing_folders}')

        # in UTC - checks between 8 and 9 am EST
        if 12 <=datetime.now().hour <= 13:

            # get job history
            job_hist = paginate_through_jobruns(rtt_glue_job_name,400,200)

            failed_jobs = get_failed_jobs(job_hist)

            # check missing systems for failures
            for fj in missing_folders:
                if fj in failed_jobs:
                    log.info(f'## RTT GLUE JOB HAD FAILURE: {fj}')
                else:
                    log.info(f'## Creating Dummy File - No Changed Data for : {fj}')
                    arguments= {'--source_system_name': fj[0],'--source_system_table_name': fj[1]}
                    response = glue_client.start_job_run(JobName = 'generate-dummy-file-glue-job',
                                                          Arguments=arguments)
                    log.info('## GLUE JOB RUN ID: ' + response['JobRunId'])