import glob
import logging
import os


def create_topic(project_id, topic_id):
    """Create a new Pub/Sub topic."""
    # [START pubsub_quickstart_create_topic]
    # [START pubsub_create_topic]
    from google.cloud import pubsub_v1

    # TODO(developer)
    # project_id = "your-project-id"
    # topic_id = "your-topic-id"

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    topic = publisher.create_topic(topic_path)

    print("Created topic: {}".format(topic.name))


def publish_messages(project_id, topic_id, data_file):
    """Publishes multiple messages to a Pub/Sub topic."""
    # [START pubsub_quickstart_publisher]
    # [START pubsub_publish]
    from google.cloud import pubsub_v1

    # TODO(developer)
    # project_id = "your-project-id"
    # topic_id = "your-topic-id"

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    for file_name in os.listdir(data_file):
        with open(os.path.join(data_file, file_name), 'r') as fp:  # open in readonly mode
            Lines = fp.readlines()
            for data in Lines:
                # Data must be a bytestring
                data = data.encode("utf-8")
                # When you publish a message, the client returns a future.
                future = publisher.publish(topic_path, data)
                print(future.result())

    # The `topic_path` method creates a fully qualified identifier
    # in the form `projects/{project_id}/topics/{topic_id}`
    topic_path = publisher.topic_path(project_id, topic_id)

    print(f"Published messages to {topic_path}.")
    # [END pubsub_quickstart_publisher]
    # [END pubsub_publish]


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    # create_topic("de2020", "game_data")
    publish_messages("de2020", "game_data", "C:\Postdoc\DE2020\DE2020\lab8\stpipeline1withpubsub\data\game")
