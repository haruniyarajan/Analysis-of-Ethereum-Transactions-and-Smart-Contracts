"""Top Ten Most Popular Services
"""
import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("Ethereum")\
        .getOrCreate()

    def good_lines_trans(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            int(fields[3])
            return True
        except:
            return False
        
    def good_lines_contracts(line):
        try:
            fields = line.split(',')
            if len(fields)!=6:
                return False
            else:
                return True
        except:
            return False
    
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    
    transactions = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    contracts = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv")
    
    transactions = transactions.filter(good_lines_trans)
    contracts = contracts.filter(good_lines_contracts)
    
    trans_by_category =transactions.map(lambda x: (x.split(',')[6], int(x.split(',')[7]))).reduceByKey(lambda x, y: x + y)
    contract_counts =contracts.map(lambda x: (x.split(',')[0],1))
    joined_data=trans_by_category .join(contract_counts)
    top_popular_services=joined_data.map(lambda x: (x[0], x[1][0])).takeOrdered(10, key=lambda l: -1*l[1])
 
    
    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_top_popular_services' + date_time + '/top_popular_services.txt')
    my_result_object.put(Body=json.dumps(top_popular_services))
    
    spark.stop()
    


    