import producer_server
import logging

def run_kafka_server():
    # Get the json file path
    input_file = "police-department-calls-for-service.json"

    # Create a Producer Server.
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="com.udacity.crime.statistics.LA",
        bootstrap_servers="localhost:9092",
        client_id=None
    )

    if producer.bootstrap_connected():
        logging.info("Bootstrap server connected")

    return producer

# Driver for Kafka Producer nd feed data to topic
if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)
    console = logging.StreamHandler()
    producer = run_kafka_server()
    try:
        producer.generate_data()
    except:
        producer.counter = 0
        producer.flush()
        producer.close()