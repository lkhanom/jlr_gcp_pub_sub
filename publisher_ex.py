from google.cloud import pubsub_v1

# TODO(developer): Choose an existing topic.
project_id = "jlr-dl-cat-training"
topic_id = "temp-2022-jlr-de-topic-room_3"
message_key = "room-3"

publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
# Sending messages to the same region ensures they are received in order
# even when multiple publishers are used.
client_options = {"api_endpoint": "us-east1-pubsub.googleapis.com:443"}
publisher = pubsub_v1.PublisherClient(
    publisher_options=publisher_options, client_options=client_options
)
# The `topic_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/topics/{topic_id}`
topic_path = publisher.topic_path(project_id, topic_id)


for message in [
    ("message1-lippe", message_key),
    # ("message2", message_key),
    # ("message3", message_key),
    # ("message4", message_key),
    # ("message5", message_key),
    # ("message6", message_key),
    # ("message7", message_key),
    # ("message8", message_key),
    # ("message9", message_key),
]:
    # Data must be a bytestring
    data = message[0].encode("utf-8")
    ordering_key = message[1]
    # When you publish a message, the client returns a future.
    future = publisher.publish(topic_path, data=data, ordering_key=ordering_key)
    print(future.result())

print(f"Published messages with ordering keys to {topic_path}.")