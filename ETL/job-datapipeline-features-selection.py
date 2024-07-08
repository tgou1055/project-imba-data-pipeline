
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
# read metadata from database='imba'
orders = glueContext.create_dynamic_frame. \
    from_catalog(database="cfn_imba_database_parquet", table_name="orders", transformation_ctx="S3_out")
orders_df = orders.toDF()
print("data loading complete")
#orders_df.show(5)
# read metadata from database='imba'
order_products = glueContext.create_dynamic_frame. \
    from_catalog(database="cfn_imba_database_parquet", table_name="order_products", transformation_ctx="S3_out")

# convert glue dynamic frame to 
order_products_df = order_products.toDF()

print("data loading complete")
#order_products_df.show(5)
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.functions import avg, sum, min, max, round, count, when, col, countDistinct, desc, asc
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
"""
Q1. Create a table called order_products_prior by using the last SQL query you created from the
previous assignment. It should be similar to below (note you need to replace the s3 bucket
name “imba” to yours own bucket name):

    CREATE TABLE order_products_prior AS
        (SELECT a.
        *
        ,
        b.product_id,
        b.add_to_cart_order,
        b.reordered
        FROM orders a
        JOIN order_products b
        ON a.order_id = b.order_id
        WHERE a.eval_set = 'prior')
"""
order_products_prior_df = orders_df.filter(
                          orders_df.eval_set == 'prior').join(order_products_df, \
                          orders_df.order_id == order_products_df.order_id, 'inner').select(
                          orders_df["*"], order_products_df.product_id, order_products_df.add_to_cart_order, order_products_df.reordered)

parquet_file_path = "s3://tgou1055-imba-sls-deployment/features/order_products_prior/"
order_products_prior_df.write.mode("overwrite").parquet(parquet_file_path)

print("transformation job finished")
#show info of df
order_products_prior_df
#put this feature table to the catalog
dynamic_frame = DynamicFrame.fromDF(order_products_prior_df, glueContext, "dynamic_frame")

s3output = glueContext.getSink(
  path="s3://tgou1055-imba-sls-deployment/features/order_products_prior/",
  connection_type="s3",
  updateBehavior="UPDATE_IN_DATABASE",
  partitionKeys=[],
  compression="snappy",
  enableUpdateCatalog=True,
  transformation_ctx="s3output",
)
s3output.setCatalogInfo(
  catalogDatabase="cfn_imba_database_parquet", catalogTableName="order_products_prior"
)
s3output.setFormat("glueparquet")
s3output.writeFrame(dynamic_frame)
print("catalog logging complete")
# parquet_file_path = "s3://tgou1055-imba-sls-deployment/features/order_products_prior/"
# order_products_prior_df = spark.read.parquet(parquet_file_path)
# order_products_prior_df.printSchema()
"""
Q2.Create a SQL query (user_features_1). Based on table orders, for each user, calculate the
max order_number, the sum of days_since_prior_order and the average of
days_since_prior_order.

SELECT user_id,
       MAX(order_number) as max_order_number, 
       CAST(SUM(days_since_prior) AS INT) as sum_days_prior,
       ROUND(AVG(days_since_prior),2) as avg_days_prior
FROM orders 
GROUP BY user_id 
ORDER BY user_id;

"""
user_features_1_df = orders_df.withColumn("days_since_prior", orders_df["days_since_prior"].cast(IntegerType()) ). \
    groupBy("user_id").agg(
    max("order_number").alias("max_order_number"),
    sum("days_since_prior").alias("sum_days_prior"),
    avg("days_since_prior").alias("avg_days_prior"))

parquet_file_path = "s3://tgou1055-imba-sls-deployment/features/user_features_1/"
user_features_1_df.write.mode("overwrite").parquet(parquet_file_path)

print("transformation job finished")
# #put this feature table to the catalog
# dynamic_frame = DynamicFrame.fromDF(user_features_1_df, glueContext, "dynamic_frame")

# s3output = glueContext.getSink(
#   path="s3://tgou1055-imba-sls-deployment/features/user_features_1/",
#   connection_type="s3",
#   updateBehavior="UPDATE_IN_DATABASE",
#   partitionKeys=[],
#   compression="snappy",
#   enableUpdateCatalog=True,
#   transformation_ctx="s3output",
# )
# s3output.setCatalogInfo(
#   catalogDatabase="cfn_imba_database_parquet", catalogTableName="user_features_1"
# )
# s3output.setFormat("glueparquet")
# s3output.writeFrame(dynamic_frame)
# print("catalog logging complete")
"""
Q3.Create a SQL query (user_features_2). Similar to above, based on table
order_products_prior, for each user calculate the total number of products, total number of
distinct products, and user reorder ratio(number of reordered = 1 divided by number of
order_number > 1)


WITH user_ratio AS (SELECT user_id, 
			   COUNT(*) as product_bought, 
               COUNT(DISTINCT(product_id)) as unique_product_bought, 
			   COUNT(CASE WHEN reordered = 1 THEN 1 ELSE NULL END) as num_reordered, 
               COUNT(CASE WHEN order_number > 1 THEN 1 ELSE NULL END) as num_order_number
		    FROM order_products_prior
		    GROUP BY user_id
		    ORDER BY user_id) SELECT user_id, 
 					     product_bought, 
					     unique_product_bought, 
					     num_reordered, num_order_number, 
					     ROUND(CAST(num_reordered AS DOUBLE) / num_order_number ,4) AS reorder_ratio 
				      FROM user_ratio

"""

user_features_2_df = order_products_prior_df.groupBy("user_id").agg(
    count("*").alias("num_product_bought"),
    countDistinct("product_id").alias("num_distinct_product_bought"),
    round(count(when(col("reordered") == 1, True)) / count(when(col("order_number") > 1, True)),4).alias("reordered_ratio")
)

parquet_file_path = "s3://tgou1055-imba-sls-deployment/features/user_features_2/"
user_features_2_df.write.mode("overwrite").parquet(parquet_file_path)
print("transformation job finished")
# #put this feature table to the catalog
# dynamic_frame = DynamicFrame.fromDF(user_features_2_df, glueContext, "dynamic_frame")

# s3output = glueContext.getSink(
#   path="s3://tgou1055-imba-sls-deployment/features/user_features_2/",
#   connection_type="s3",
#   updateBehavior="UPDATE_IN_DATABASE",
#   partitionKeys=[],
#   compression="snappy",
#   enableUpdateCatalog=True,
#   transformation_ctx="s3output",
# )
# s3output.setCatalogInfo(
#   catalogDatabase="cfn_imba_database_parquet", catalogTableName="user_features_2"
# )
# s3output.setFormat("glueparquet")
# s3output.writeFrame(dynamic_frame)
# print("catalog logging complete")
"""
Q4:
    Create a SQL query (up_features). Based on table order_products_prior, for each user and
    product, calculate the total number of orders, minimum order_number, maximum
    order_number and average add_to_cart_order.

    SELECT user_id, 
       product_id, 
       COUNT(*) as num_of_orders, 
       MIN(order_number) as min_order_num, 
       MAX(order_number) as max_order_num, 
       ROUND(AVG(add_to_cart_order),2) as seq_add_to_order
    FROM order_products_prior
    GROUP BY user_id, product_id
    ORDER BY user_id, product_id;
"""

up_features_df = order_products_prior_df.groupBy("user_id","product_id").agg(
    count("*").alias("number_of_orders"),
    min("order_number").alias("prod_min_order_num"),
    max("order_number").alias("prod_max_order_num"),
    round(avg("add_to_cart_order"),2).alias("seq_add_to_order")
)

parquet_file_path = "s3://tgou1055-imba-sls-deployment/features/up_features/"
up_features_df.write.mode("overwrite").parquet(parquet_file_path)
print("transformation job finished")
"""
Q5. Create a SQL query (prd_features). Based on table order_products_prior, first write a sql
query to calculate the sequence of product purchase for each user, and name it
product_seq_time. Then on top of this query, for each product, calculate the count, sum of reordered, count of
product_seq_time = 1 and count of product_seq_time = 2.

WITH product_seq AS (SELECT user_id, 
			    order_number, 
			    product_id,
			    ROW_NUMBER() OVER (PARTITION BY user_id, product_id ORDER BY order_number ASC) AS product_seq_time,
			    reordered
		     FROM order_products_prior
		     ORDER BY user_id, order_number, product_seq_time) SELECT product_id, 
									      COUNT(*) AS num_product_ordered, 
									      SUM(reordered) as sum_reordered, 
									      COUNT(CASE WHEN product_seq_time = 1 THEN 1 ELSE NULL END) as seq_is_one, 
									      COUNT(CASE WHEN product_seq_time = 2 THEN 1 ELSE NULL END) as seq_is_two
									      FROM product_seq
									      GROUP BY product_id
									      ORDER BY product_id;
"""

# Define a Window specification to partition and order the data
windowSpec = Window.partitionBy("user_id", "product_id").orderBy("order_number")
prd_features_df = order_products_prior_df.withColumn("product_seq_time", row_number().over(windowSpec))

prd_features_df = prd_features_df.groupBy("product_id").agg (
                count("*").alias("num_product_ordered"),
                sum("reordered").alias("sum_reordered"),
                count(when(col("product_seq_time") == 1, True)).alias("seq_is_one"),
                count(when(col("product_seq_time") == 2, True)).alias("seq_is_two")
)

parquet_file_path = "s3://tgou1055-imba-sls-deployment/features/prd_features/"
prd_features_df.write.mode("overwrite").parquet(parquet_file_path)
print("transformation job finished")
parquet_file_path = "s3://tgou1055-imba-sls-deployment/features/prd_features/"
prd_features_df = spark.read.parquet(parquet_file_path)
#prd_features_df.printSchema()
# stop sparkSession
spark.stop()
job.commit()