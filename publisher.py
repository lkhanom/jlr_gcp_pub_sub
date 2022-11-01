from google.cloud import pubsub_v1

# TODO (change it to yours)
project_id = "jlr-dl-cat-training"
topic_id = "temp-2022-jlr-de-topic-lippe"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

for n in range(1, 10):
    data_str = f"Message number {n}"
    # Data must be a bytestring
    data = data_str.encode("utf-8")
    # When you publish a message, the client returns a future.
    # to learn more visit https://docs.python.org/3/library/asyncio-future.html
    future = publisher.publish(topic_path, data)
    print(future.result())

print(f"Published messages to {topic_path}.")