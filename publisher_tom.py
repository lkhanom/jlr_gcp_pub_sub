from datetime import datetime
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1

# TODO (change it to yours)
project_id = "jlr-dl-cat-training"
topic_id = "temp-2022-jlr-de-topic-room_3"
subscription_id = "temp-2022-jlr-de-subscription-room-3-lippe"
ordering_key = "room-3"
timeout = 15.0

publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
# Sending messages to the same region ensures they are received in order
# even when multiple publishers are used.
client_options = {"api_endpoint": "us-east1-pubsub.googleapis.com:443"}
publisher = pubsub_v1.PublisherClient(
    publisher_options=publisher_options, client_options=client_options
)

subscriber = pubsub_v1.SubscriberClient()
# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_id}`
subscription_path = subscriber.subscription_path(project_id, subscription_id)
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    print(f"Received {message}.")
    message.ack()

# publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)
print("1 - Send message \n2 - Read messages")
selection = int(input("Please select: "))
if selection == 1:
    data_str = input("Message: ") + ", from lippe, "+str(datetime.now())
    # Data must be a bytestring
    data = data_str.encode("utf-8")
    # When you publish a message, the client returns a future.
    # to learn more visit https://docs.python.org/3/library/asyncio-future.html
    future = publisher.publish(topic_path, data=data, ordering_key=ordering_key)
    print(future.result())
    print(f"Published messages to {topic_path}.")
elif selection == 2:
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")
    with subscriber:
        try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result()  # Block until the shutdown is complete.