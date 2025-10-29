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

# populating the queue (run only once in flow)
@task
def start_populate_queue(queue_url_start):
    try:
        response = requests.post(queue_url_start)
        response.raise_for_status()
        payload = response.json()
        logging.info(f"finished starting queue: {payload}") 
        return payload # have all 21 items in queue
    except Exception as e:
        print(f"Error starting queue: {e}")
        logging.warning(f"Error starting queue: {e}")
        raise e


# reading in each message as tuple (order num, word)
@task
def read_store_messages(sqs_url):
    # time constraint
    deadline = time.time() + 16 * 60  # 960 sec
    results = [] # store items in tuple (order num, word)
    
    try:
        # if we haven't reached 21 items grabbed from queue, and if we're under 960 sec:
        while len(results) < 21 and time.time() < deadline:
            # get the attributes listed from queue items
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

            # store messages from response here
            messages = response.get("Messages", [])

            if not messages:
                continue

            # this is how we get the order num and word to put in tuple, then append to results list
            for message in messages:
                temp = (int(message['MessageAttributes']['order_no']['StringValue']), message['MessageAttributes']['word']['StringValue'])
                results.append(temp)
                print(temp)

                # need to delete item from queue once seen
                sqs.delete_message(
                    QueueUrl=sqs_url,
                    ReceiptHandle=message["ReceiptHandle"]
                )

        return results

    except Exception as e:
        raise e


# sorting words based on first tuple item order num (0, 1, 2, etc) in list results
@task
def sort_results(results):
    if not results:
        # somehow there's nothing in results from read_store_messages
        return []

    results.sort(key=lambda pair: pair[0]) # sort based on (this, ___) in list results
    return results


# assembling tuples number in order into string
@task
def assemble_fragments(sorted_fragments):
    if not sorted_fragments:
        # somehow list is empty - look at sort_results
        return ""
    
    # loop over items in sorted list as a new list of strings, then assemble in string builder
    words = [fragment for (_, fragment) in sorted_fragments]
    result = " ".join(words)
    result = re.sub(r"\s+([,.;:!?])", r"\1", result)
    return result


# send string assembled into the submit queue
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

        # debugging
        print("Submission OK. MessageId:", response.get("MessageId"))
        print(f"Response: {response}")
        # print(f"{phrase}")

    except Exception as e:
        raise e



### DAG ###

# DAG of all the above task methods
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
    trigger_task = start_populate_queue(queue_url) # start queue population
    fragments_task = read_store_messages(sqs_url) # grab messages in their raw-er form
    sorted_task = sort_results(fragments_task) # sort the words in tuples list
    assembled_task = assemble_fragments(sorted_task) # assemble into word
    submit_task = send_solution(my_uvaid, assembled_task, "airflow") # submit to submit queue

    # order in DAG
    trigger_task >> fragments_task >> sorted_task >> assembled_task >> submit_task


## CALL DAG ## 
dag = get_queue_submit()