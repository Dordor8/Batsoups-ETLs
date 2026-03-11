import os
from kafka import KafkaConsumer

topic = os.getenv("TOPIC")
bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS")
group_id = os.getenv("GROUP_ID")

output_file = os.getenv("OUTPUT_FILE")


consumer = KafkaConsumer(topic,
                         bootstrap_servers=[bootstrap_servers.split(',')],
                         group_id=group_id)

def extract():
    message = consumer.poll(max_records=1)

    with open(output_file, "w") as file:
        file.write(str(message))

    consumer.commit()

if __name__ == "__main__":
    extract()