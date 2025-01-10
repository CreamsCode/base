import boto3
import json

class SQSManager:
    def __init__(self, queue_url, aws_access_key_id, aws_secret_access_key, region_name="us-east-1"):
        self.queue_url = queue_url
        self.client = boto3.client(
            'sqs',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )

    def send_message(self, message_body):
        response = self.client.send_message(
            QueueUrl=self.queue_url,
            MessageBody=json.dumps(message_body)
        )
        return response
