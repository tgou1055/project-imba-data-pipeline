
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
 # creating features dataframes from existing athena catelog
up_features = glueContext.create_dynamic_frame_from_options(connection_type = "parquet", \
                    connection_options = {"paths": ["s3://tgou1055-imba-sls-deployment/features/up_features/"]})
prd_features = glueContext.create_dynamic_frame_from_options(connection_type = "parquet", \
                    connection_options = {"paths": ["s3://tgou1055-imba-sls-deployment/features/prd_features/"]})
user_features_1 = glueContext.create_dynamic_frame_from_options(connection_type = "parquet", \
                    connection_options = {"paths": ["s3://tgou1055-imba-sls-deployment/features/user_features_1/"]})
user_features_2 = glueContext.create_dynamic_frame_from_options(connection_type = "parquet", \
                    connection_options = {"paths": ["s3://tgou1055-imba-sls-deployment/features/user_features_2/"]})

print("loading data complete")
# up_features.printSchema()
# prd_features.printSchema()
# user_features_1.printSchema()
# user_features_2.printSchema()
# Join user features together
users = Join.apply(user_features_1.rename_field('user_id','user_id1'), \
                   user_features_2, 'user_id1', 'user_id').drop_fields(['user_id1'])
#users.printSchema()
print("user features joined")
user_product = Join.apply(up_features, users.rename_field('user_id','user_id1'),'user_id','user_id1').drop_fields(['user_id1'])
#df.printSchema()
print("user features and user_product features joined")
# Join everything together
df = Join.apply(user_product, prd_features.rename_field('product_id','product_id1'), 'product_id','product_id1').drop_fields(['product_id1'])
print("all features joined")
# convert glue dynamic dataframe to spark dataframe
df_spark = df.toDF()
df_spark.repartition(1).write.mode('overwrite').format('parquet').save("s3://tgou1055-imba-sls-deployment/features/features_aggregation/", header = 'true')
print("data saving complete")

#put this feature table to the catalog
dynamic_frame = DynamicFrame.fromDF(df_spark, glueContext, "dynamic_frame")

s3output = glueContext.getSink(
  path="s3://tgou1055-imba-sls-deployment/features/features_aggregation/",
  connection_type="s3",
  updateBehavior="UPDATE_IN_DATABASE",
  partitionKeys=[],
  compression="snappy",
  enableUpdateCatalog=True,
  transformation_ctx="s3output",
)
s3output.setCatalogInfo(
  catalogDatabase="cfn_imba_database_parquet", catalogTableName="features_aggregation"
)
s3output.setFormat("glueparquet")
s3output.writeFrame(dynamic_frame)
print("catalog logging complete")

spark.stop()
job.commit()