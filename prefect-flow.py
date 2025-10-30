# prefect flow goes here

import requests
import boto3
from prefect import flow, task, get_run_logger
import time
import re
import json

my_uvaid = "rkf9wd"
queue_url = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{my_uvaid}"
sqs_url = "https://sqs.us-east-1.amazonaws.com/440848399208/rkf9wd"
submit_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"

path_persist = "/tmp/fragments.json"

sqs = boto3.client("sqs", region_name="us-east-1")


### HELPERS ###

# helper get queue attributes from class
# def get_queue_attributes2(sqs_url):
#     # logger = get_run_logger()
#     try:
#         response = sqs.get_queue_attributes(
#             QueueUrl=sqs_url,
#             AttributeNames=['All']
#         )
#         return response
#     # logger = get_run_logger()
#     except Exception as e:
#         print(f"Error getting queue attributes: {e}")
#         # logger = get_run_logger()
#         raise e


### TASKS ###

# writing items from results fragments list into tmp/fragments.json
# @task
# def persist_fragments(results, path=path_persist):
#     logger = get_run_logger()
#     serializable = [[int(n), str(w)] for (n, w) in results]

#     with open(path, "w") as f:
#         json.dump(serializable, f)

#     logger.info(f"Persisted {len(results)} fragments to {path}")
#     return path


# loading fragments into json tmp/fragments.json
# @task
# def load_fragments(path=path_persist):
#     logger = get_run_logger()
#     with open(path, "r") as f:
#         data = json.load(f)

#     loaded = [(int(n), w) for n, w in data]
#     logger.info(f"Loaded {len(loaded)} fragments from {path}")
#     return loaded


# populating the queue (run only once in flow)
@task
def start_populate_queue(queue_url_start):
    logger = get_run_logger()
    try:
        response = requests.post(queue_url_start)
        response.raise_for_status()
        payload = response.json()
        logger.info(f"Scatter API payload: {payload}")
        return payload # have all 21 items in queue
    except Exception as e:
        print(f"Error starting queue: {e}")
        logger.warning(f"Couldn't finish starting and loading queue: {e}")
        raise e
    

# reading in each message as tuple (order num, word)
@task
def read_store_messages(sqs_url):
    logger = get_run_logger()
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
            logger.info(f"Queue status: {attrs}")

            response = sqs.receive_message(
                QueueUrl=sqs_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20,
                VisibilityTimeout=60,
                MessageAttributeNames=["All"],
            )

            logger.info(f"Response completed: received message: {response}")

            # store messages from response here
            messages = response.get("Messages", [])

            if not messages:
                continue

            # this is how we get the order num and word to put in tuple, then append to results list
            for message in messages:
                temp = (int(message['MessageAttributes']['order_no']['StringValue']), message['MessageAttributes']['word']['StringValue'])
                results.append(temp)
                print(temp)

                logger.info(f"Completed reading message of {message}")
                # need to delete item from queue once seen
                sqs.delete_message(
                    QueueUrl=sqs_url,
                    ReceiptHandle=message["ReceiptHandle"]
                )

        # persist and load into json:
        serializable = [[int(n), str(w)] for (n, w) in results]

        with open(path_persist, "w") as f:
            json.dump(serializable, f)

        with open(path_persist, "r") as f:
            data = json.load(f)
        loaded = [(int(n), w) for n, w in data]


        logger.info(f"Completed obtaining message fragments in list: {results}")
        # return results
        return loaded

    except Exception as e:
        logger.warning(f"Issue reading message: {e}")
        raise e
    

# sorting words based on first tuple item order num (0, 1, 2, etc) in list results
@task
def sort_results(results):
    logger = get_run_logger()
    if not results:
        logger.warning(f"There is no results from last method read_store_messages")
        # somehow there's nothing in results from read_store_messages
        return []
    
    results.sort(key=lambda pair: pair[0]) # sort based on (this, ___) in list results
    logger.info(f"sorted results: {results}")
    return results


# assembling tuples number in order into string
@task
def assemble_fragments(sorted_fragments):
    logger = get_run_logger()
    if not sorted_fragments:
        # somehow list is empty - look at sort_results
        logger.warning(f"There is no sorted_fragments")
        return ""
    
    # loop over items in sorted list as a new list of strings, then assemble in string builder
    words = [fragment for (_, fragment) in sorted_fragments]
    result = " ".join(words)
    result = re.sub(r"\s+([,.;:!?])", r"\1", result)
    logger.info(f"assembled fragments into string, stripping of weird phrasing issues: {result}")
    return result


# send string assembled into the submit queue
@task
def send_solution(uvaid, phrase, platform):
    logger = get_run_logger()
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
            logger.warning(f"Submission failed with HTTP {status_code}, full response: {response}")
            raise RuntimeError(f"Submission failed with HTTP {status_code}, full response: {response}")

        # debugging
        print("Submission OK. MessageId:", response.get("MessageId"))
        print(f"Response: {response}")
        # print(f"{phrase}")

    except Exception as e:
        logger.warning(f"Couldn't send solution due to error: {e}")
        raise e


### FLOW ###

# flow of all the above task methods
@flow
def get_queue_submit(do_scatter_now=False):
    logger = get_run_logger()
    logger.info(f"-----INITIATING PREFECT FLOW FOR QUEUE-----\n")

    if do_scatter_now:
        logger.info("* Starting population of queue:")
        start_populate_queue(queue_url)
    else:
        logger.info("* Skipping population step; using existing queue contents")


    logger.info(f"* Reading then storing messages in list of di-tuples:")
    fragments = read_store_messages(sqs_url)

    # logger.info(f"* Persisting and Loading message fragments into json tmp/fragments.json:")
    # path = persist_fragments(fragments)
    # loaded = load_fragments(path)

    logger.info(f"* Sorting fragments of message:")  
    sorted_fragments = sort_results(fragments)


    # debugging:
    logger.info(f"* DEBUG: we have {len(sorted_fragments)} fragments after sort")
    logger.info(f"* DEBUG: order numbers = {[n for (n, _) in sorted_fragments]}")
    logger.info(f"* DEBUG: words = {[w for (_, w) in sorted_fragments]}")


    logger.info(f"* Assembling sorted message fragments into final message string:")
    assembled_string = assemble_fragments(sorted_fragments)

    logger.info(f"* Submitting string to submission url:")
    send_solution(my_uvaid, assembled_string, "prefect")




### MAIN ###

if __name__ == "__main__":
    # returned_info = start_populate_queue(queue_url)
    # attributes = get_queue_attributes2(sqs_url)
    # print(attributes['Attributes']['ApproximateNumberOfMessages'])
    # print(attributes['Attributes']['ApproximateNumberOfMessagesNotVisible'])
    # print(attributes['Attributes']['ApproximateNumberOfMessagesDelayed'])
    # fragments = read_store_messages(sqs_url)
    # sorted_fragments = sort_results(fragments)
    # assembled_string = assemble_fragments(sorted_fragments)
    # print(assembled_string)

    # call flow
    get_queue_submit(do_scatter_now=False)
