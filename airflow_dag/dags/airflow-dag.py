# airflow DAG goes here

from airflow.decorators import dag, task
from datetime import datetime, timedelta
import boto3
import requests
import time
import re
import logging 


my_uvaid = "rkf9wd"
queue_url = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{my_uvaid}"
sqs_url = "https://sqs.us-east-1.amazonaws.com/440848399208/rkf9wd"
submit_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"

sqs = boto3.client("sqs", region_name="us-east-1")


### TASKS ###

@task
def start_populate_queue(queue_url_start):
    try:
        response = requests.post(queue_url_start)
        response.raise_for_status()
        payload = response.json()
        logging.info(f"finished starting queue: {payload}")
        return payload
    except Exception as e:
        print(f"Error starting queue: {e}")
        logging.warning(f"Error starting queue: {e}")
        raise e


@task
def read_store_messages(sqs_url):
    deadline = time.time() + 16 * 60  # 960 sec
    results = []
    
    try:
        while len(results) < 21 and time.time() < deadline:
            attrs = sqs.get_queue_attributes(
                QueueUrl=sqs_url,
                AttributeNames=[
                    "ApproximateNumberOfMessages",
                    "ApproximateNumberOfMessagesNotVisible",
                    "ApproximateNumberOfMessagesDelayed",
                ],
            )

            response = sqs.receive_message(
                QueueUrl=sqs_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20,
                VisibilityTimeout=60,
                MessageAttributeNames=["All"],
            )


            messages = response.get("Messages", [])

            if not messages:
                continue

            for message in messages:
                temp = (int(message['MessageAttributes']['order_no']['StringValue']), message['MessageAttributes']['word']['StringValue'])
                results.append(temp)
                print(temp)

                sqs.delete_message(
                    QueueUrl=sqs_url,
                    ReceiptHandle=message["ReceiptHandle"]
                )

        return results

    except Exception as e:
        raise e


@task
def sort_results(results):
    if not results:
        return []

    results.sort(key=lambda pair: pair[0])
    return results


@task
def assemble_fragments(sorted_fragments):
    if not sorted_fragments:
        return ""
    
    words = [fragment for (_, fragment) in sorted_fragments]
    result = " ".join(words)
    result = re.sub(r"\s+([,.;:!?])", r"\1", result)
    return result


@task
def send_solution(uvaid, phrase, platform):
    try:
        response = sqs.send_message(
            QueueUrl=submit_url,
            MessageBody=f"solution for {uvaid} via {platform}",
            MessageAttributes={
                'uvaid': {
                    'DataType': 'String',
                    'StringValue': uvaid
                },
                'phrase': {
                    'DataType': 'String',
                    'StringValue': phrase
                },
                'platform': {
                    'DataType': 'String',
                    'StringValue': platform
                }
            }
        )
        status_code = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        print("SQS send_message status:", status_code)

        if status_code != 200:
            raise RuntimeError(f"Submission failed with HTTP {status_code}, full response: {response}")

        print("Submission OK. MessageId:", response.get("MessageId"))
        print(f"Response: {response}")
        # print(f"{phrase}")

    except Exception as e:
        raise e



### DAG ###

@dag(
    dag_id="dp2_pipeline",
    start_date=datetime(2025, 10, 28),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    # tags=["dp2", "sqs", "uva"],
)
def get_queue_submit():
    trigger_task = start_populate_queue(queue_url)
    fragments_task = read_store_messages(sqs_url)
    sorted_task = sort_results(fragments_task)
    assembled_task = assemble_fragments(sorted_task)
    submit_task = send_solution(my_uvaid, assembled_task, "airflow")

    trigger_task >> fragments_task >> sorted_task >> assembled_task >> submit_task


## CALL DAG ## 
dag = get_queue_submit()