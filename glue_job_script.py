import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


# Database and table names
source_database = "cfn_imba_database"
source_table = "aisles"
#target_table = "cfn_database.cfn_parquet_product_clearned"
target_path = "s3://tgou1055-imba-sls-test-event/data-parquet/aisles/"

# Read data from the catalog
dyf = glueContext.create_dynamic_frame.from_catalog(database = source_database, table_name = source_table)
#dyf.printSchema()
df = dyf.toDF()

# Write data to s3 local with parquet
#filtered_df.write.mode("overwrite").partitionBy("department_id").parquet(target_path)
df.write.mode("overwrite").parquet(target_path)

spark.stop()
job.commit()