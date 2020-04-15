import logging
from kafka import KafkaConsumer

topic_name = "com.udacity.crime.statistics.LA"

if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    
    c = KafkaConsumer(
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest',
            consumer_timeout_ms=1000)
    c.subscribe([topic_name])
    
    for message in c:
        if message is None:
            print(f"No message received by consumer")
        else:
            print(message.value)