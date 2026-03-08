import json
import boto3
import os

sns = boto3.client("sns")
TOPIC_ARN = os.environ["TOPIC_ARN"]

def lambda_handler(event, context):

    for record in event["Records"]:

        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
        size = record["s3"]["object"].get("size", 0)
        event_time = record["eventTime"]
        event_name = record["eventName"]

        message = {
            "bucket": bucket,
            "file_name": key,
            "file_size": size,
            "event_time": event_time,
            "event_type": event_name
        }

        sns.publish(
            TopicArn=TOPIC_ARN,
            Message=json.dumps(message),
            Subject="S3 File Upload Event"
        )

    return {"statusCode": 200}