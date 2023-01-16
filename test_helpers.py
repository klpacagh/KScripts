import unittest
from unittest import mock
from unittest.mock import patch
from moto import mock_glue, mock_s3
from helpers import get_db_instances, create_table_dict, remove_null_fields, dropNullTableRecords, addCreatedAndUpdatedAt
import pandas as pd
from pandas.testing import assert_frame_equal
from pyspark.sql import SparkSession
import os
import sys
import boto3
from datetime import date
import json

# os.environ["PYTHON_HOME"] = r"C:\Python310"
# os.environ["PYSPARK_PYTHON"] = r"D:\Users\kevinle\.virtualenvs\klepga_repo-sTDDsAaC\Scripts"
# os.environ["PYSPARK_PYTHON"] = r"D:\Users\kevinle\.virtualenvs\klepga_repo-sTDDsAaC\Scripts"
# os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Users\kevinle\.virtualenvs\klepga_repo-sTDDsAaC\Scripts"

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

class TestHelpers(unittest.TestCase):

    '''
    def test_dropNullTableRecords(self):
        df1=pd.DataFrame({'id':[1,2,3], 'col_a':['a',None,'c']})
        test_dict = {}
        test_dict['test'] = df1
        new_df = dropNullTableRecords(test_dict)
        assert_frame_equal(new_df['test'].reset_index(drop=True), pd.DataFrame({'id':[1,3], 'col_a':['a','c']}))

    def test_addCreatedAndUpdatedAt(self):
        df1=pd.DataFrame({'id':[1,2,3], 'col_a':['a',None,'c']})
        test_dict = {}

        spark = SparkSession.builder \
            .master("local") \
            .getOrCreate()
        #Create PySpark DataFrame from Pandas
        sparkDF=spark.createDataFrame(df1) 
        test_dict['test'] = sparkDF

        # addCreatedAndUpdatedAt takes in a spark df within the dict, not pandas
        new_dict = addCreatedAndUpdatedAt(test_dict)
        self.assertEqual(['id', 'col_a', 'created_at', 'updated_at'], new_dict['test'].columns)
    '''
    @mock_s3
    def test_remove_null_fields(self, ):
        # remove null/none fields before writing to S3
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket="refined_bucket")
        todays_date =str(date.today()).replace("-","/")

        df1=pd.DataFrame({'id':[1,2,3], 'col_a':[None,None,'c']})

        spark = SparkSession.builder \
            .master("local") \
            .getOrCreate()

        #Create PySpark DataFrame from Pandas
        sparkDF=spark.createDataFrame(df1) 

        remove_null_fields(sparkDF, "refined_bucket", todays_date)

        # get files written to bucket from remove_null_fields function
        objects = s3.list_objects_v2(Bucket="refined_bucket").get("Contents")

        # get first iterable file and read contents
        resp = s3.get_object(Bucket="refined_bucket", Key=objects[0]['Key']) # content read as a string
        iterable_file_content = resp["Body"].read().decode('UTF-8').strip('\n') # comes with extra new line
        self.assertEqual("{\"id\": 3, \"col_a\": \"c\"}", iterable_file_content)

    # @patch('helpers.get_db_instances')
    # @mock_glue
    # def test_get_db_instances(self,mock_obj):
    #     mock_obj.glue_job_response_temp = 'temp'
        # print(mock_temp)
        # print(test_me("LGTM"))
        # get_db_instances("temp",200)

if __name__ == '__main__':
    unittest.main()