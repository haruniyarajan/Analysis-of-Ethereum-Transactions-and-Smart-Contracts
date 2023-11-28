"""Gas Guzzlers
"""
import os
import boto3
import json
import time
from pyspark.sql import SparkSession
from datetime import datetime


def good_lines_trans(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            float(fields[9])
            float(fields[11])
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
    

if __name__ == "__main__":
    # Create a Spark session
    spark = SparkSession.builder.appName("Ethereum").getOrCreate()

    # Configure AWS S3 access
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL'] + ':' + os.environ['BUCKET_PORT']
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
    
    # For average_gas_price calculation
    gas_prices = transactions.map(lambda line: (time.strftime("%m/%Y",time.gmtime(int(line.split(',')[11]))), 
                                          (float(line.split(',')[9]), 1)))
    gas_prices_reduced = gas_prices.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
    average_gas_price = gas_prices_reduced.map(lambda x: (x[0], x[1][0]/x[1][1])).sortByKey(ascending=True)

    # For average_gas_used calculation
    gas_used = transactions.map(lambda line: (str(line.split(',')[6]), 
                                        (time.strftime("%m/%Y",time.gmtime(int(line.split(',')[11]))), 
                                        float(line.split(',')[8]))))
    contracts = contracts.map(lambda line: (line.split(',')[0],1))
    joined_data = gas_used.join(contracts)
    gas_used_reduced = joined_data.map(lambda x: (x[1][0][0], (x[1][0][1],x[1][1]))).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
    average_gas_used = gas_used_reduced.map(lambda x: (x[0], x[1][0]/x[1][1])).sortByKey(ascending=True)
    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    
    my_bucket_resource = boto3.resource('s3',
                                         endpoint_url='http://' + s3_endpoint_url,
                                         aws_access_key_id=s3_access_key_id,
                                         aws_secret_access_key=s3_secret_access_key)

    my_result_object = my_bucket_resource.Object(s3_bucket, 'ethereum_avg_gas_price' + date_time + '/avg_gasprice.txt')
    my_result_object.put(Body=json.dumps(average_gas_price.take(100)))               
    my_result_object1 = my_bucket_resource.Object(s3_bucket, 'ethereum_avg_gas_used' + date_time + '/avg_gasused.txt')
    my_result_object1.put(Body=json.dumps(average_gas_used.take(100)))
    
    spark.stop()