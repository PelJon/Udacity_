import logging
import pykafka
import json
import time

def generate_data():
        with open(input_file) as f:
            json_array = json.load(f)
            for line in json_array:
                message = dict_to_binary(line)
                # TODO send the correct data
                producer.produce(message)
                print(message)
                time.sleep(1)
                
def dict_to_binary(json_dict):
        return json.dumps(json_dict).encode('utf-8')

if __name__ == "__main__":
    client = pykafka.KafkaClient("localhost:9092")
    print(f"topics, {client.topics}")
    input_file = "police-department-calls-for-service.json"
    producer = client.topics[b'com.udacity.crime.statistics.LA'].get_producer()
    generate_data()

