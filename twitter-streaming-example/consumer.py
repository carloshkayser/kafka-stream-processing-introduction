from kafka import KafkaConsumer
# from pymongo import MongoClient

import json

consumer = KafkaConsumer(
    'my-topic-kafka',
     bootstrap_servers=['localhost:9092'], #, 'localhost:9093', 'localhost:9094', 'localhost:9095'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# client = MongoClient('localhost:27017')
# collection = client.numtest.numtest

for message in consumer:
    message = message.value
    # collection.insert_one(message)
    # print('{} added to {}'.format(message, collection))
    print(message)