import sys
import botocore
import boto3
from botocore.exceptions import ClientError

#cron setup - 0 13 ? * MON-FRI *
#cloudwatch setup to run at 9am Mon to Fri - in UTC/GMT


def lambda_handler(event, context):
    rds = boto3.client('rds')
    lambdaFunc = boto3.client('lambda')
    print ('Trying to get Environment variable')
    try:
        funcResponse = lambdaFunc.get_function_configuration(
            FunctionName='RDSStartFunction'
       )
        DBinstance = funcResponse['Environment']['Variables']['DBInstanceName']
        print ('Starting RDS service for DBInstance : ')
    except ClientError as e:
        print(e)    
    try:
        
        response = rds.start_db_instance(
            DBInstanceIdentifier=DBinstance
        )
        print ('Success Starting up woo ') 
        return response
    except ClientError as e:
    
        print(e)    
    
    return
    
#{'message' : "Script execution completed. See Cloudwatch logs for complete output" }