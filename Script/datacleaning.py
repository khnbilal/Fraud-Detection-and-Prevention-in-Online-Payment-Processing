import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col

# Initialize the Glue context and job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load the data from the Glue Catalog table
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database = "fraud-detection-db", 
    table_name = "fraud_payment_data_csv", 
    transformation_ctx = "datasource0"
)

# Convert DynamicFrame to DataFrame to handle null values more effectively
df = datasource0.toDF()

# Drop rows with any null values in any column
cleaned_df = df.na.drop()

# Write the cleaned data back to S3 as CSV files with a .csv extension
cleaned_df.write.mode("overwrite").option("header", "true").csv("s3://fraud-detection-cleaned-data/output.csv")

# Commit the job
job.commit()
