import json
import boto3
#import csv
#import uuid
#from datetime import datetime
import os
#from pytz import timezone 


def lambda_handler(event, context):
    glue_client = boto3.client('glue')
    s3_event = event['Records'][0]['s3']
    
    bucket_name = s3_event['bucket']['name']
    object_key = s3_event['object']['key']

    glue_job_name = 'my-glue-script'
    
    try:
        response = glue_client.start_job_run(
            JobName= glue_job_name,
            Arguments={
                '--S3_BUCKET': bucket_name,
                '--S3_KEY': object_key
            }
        )
        return {
            'statusCode': 200,
            'body': json.dumps('Glue job started successfully!')
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps('Error starting Glue job: ' + str(e))
        }


# s3 = boto3.resource('s3')
# local_file = '/tmp/world_cup.csv'
# dynamodb = boto3.resource('dynamodb')
# table = dynamodb.Table(os.environ['DYNAMO_TABLE'])
# tz_sydney = timezone(os.environ['TZ_LOCAL'])
# date = datetime.now(tz_sydney).strftime("%Y-%m-%d-%H-%M-%S")

# def run(event, context):
#     print("Started\t\t" + str(event))
#     records = event['Records']
#     for record in records:
#         bucket = record['s3']['bucket']['name']
#         key = record['s3']['object']['key']
#         process_data(bucket, key, local_file)

# def process_data(bucket, key, file):
#     s3get(bucket, key, file)
#     with open(file, mode='r') as csv_file:
#         csv_reader = csv.DictReader(csv_file)
#         for row in csv_reader:
#             row_json = json.dumps(row)
#             print(row_json)
#             id=str(uuid.uuid4())
#             table.put_item(
#                 Item = {
#                     'id': id,
#                     'date': date,
#                     'content': row_json
#                 }
#                 )
    
# def s3get(bucket, key, file):
#     print(f"Download {file} to local")
#     s3.Object(bucket, key).download_file(file)
