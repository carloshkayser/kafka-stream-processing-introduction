from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer, KafkaClient

import logging
import os

log = logging.getLogger("kafka-stream-processing-introduction")

if os.path.isfile(".env"):

    from pathlib import Path  # Python 3.6+ only
    from dotenv import load_dotenv

    env_path = Path(".") / ".env"
    load_dotenv(dotenv_path=env_path)

consumer_key =  os.getenv('consumer_key')
consumer_secret =  os.getenv('consumer_secret')
access_token = os.getenv('access_token')
access_token_secret =  os.getenv('access_token_secret')

class StdOutListener(StreamListener):
    def on_data(self, data):
        producer.send("trump", data.encode('utf-8'))
        print(data)
        return True

    def on_error(self, status):
        log.ERROR(status)

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(track="trump")