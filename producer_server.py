from kafka import KafkaProducer
import json
import time
import logging


class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic
        self.counter = 0

    # Read data from json file and feed to topic
    def generate_data(self):
        with open(self.input_file, 'r') as f:
            file_data = json.load(f)
            for message in file_data:
                line = self.dict_to_binary(message)
                # TODO send the correct data
                record = self.send(topic=self.topic, value=line)
                self.counter = self.counter + 1
                logging.info(f"Record number : {self.counter}. Record : {record.get()}. Message : {line}")
                time.sleep(0.1)

    # return the json dictionary to binary
    def dict_to_binary(self, json_dict):
        return json.dumps(json_dict).encode('utf8')