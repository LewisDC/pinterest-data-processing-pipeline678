from batch_consumer import read_credentials
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os

# Adding the packages required to get data from S3
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.390,org.apache.hadoop:hadoop-aws:3.3.4 pyspark-shell"

# Creating our Spark configuration
conf = SparkConf() \
    .setAppName('S3toSpark') \
    .setMaster('local[*]')

sc = SparkContext(conf=conf)

creds = read_credentials('creds.yaml')

# Configure the setting to read from the S3 bucket
accessKeyId=creds['aws_access_key']
secretAccessKey=creds['aws_secret_key']

hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', accessKeyId)
hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') # Allows the package to authenticate with AWS

# Create our Spark session
spark = SparkSession(sc).builder.appName("s3App").getOrCreate()

# Read from the S3 bucket
s3_path = f"s3a://pinterest-data-9022bf17-8563-4cbf-9bb8-f6f0fffa033a/*.json"

df = spark.read.json(s3_path)
df.printSchema()
df.show(20, True)