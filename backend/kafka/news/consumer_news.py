from kafka import KafkaConsumer
from sklearn.preprocessing import MinMaxScaler
import json
from elasticsearch import Elasticsearch
import tensorflow as tf
import pickle
from keras.models import load_model
import warnings
from datetime import datetime, timedelta
from dateutil import parser
import sys
sys.path.append("./ver_0.3/kafka/news/utils")
from model import SentimentAnalyzer
import numpy as np


warnings.filterwarnings("ignore")

# Prepare settings for consuming data
es = Elasticsearch("http://localhost:9200")
topic_name = 'news-data-2'
default_probability = 0
highest = 5983.2
lowest = -5643.4

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)


def fetch_bitcoin_prices(date):
    # Format the date to string if necessary
    date_str = date.strftime('%Y-%m-%d')
    next_day_str = (date + timedelta(days=1)).strftime('%Y-%m-%d')

    response = es.search(index="new-finhub-data-3", body={
        "query": {
            "bool": {
                "must": [
                    {"match": {"symbol": "BINANCE:BTCUSDT"}},
                    {"range": {"timestamp": {"gte": date_str, "lt": next_day_str}}}
                ]
            }
        },
        "sort": [{"timestamp": {"order": "asc"}}],
        "size": 1
    })

    return response['hits']['hits'][0]['_source']['predicted_price'] if response['hits']['hits'] else None

# Load your pre-fitted Keras Tokenizer and Model
model_path = './ver_0.3/backend/MLTraining/training/news/model_adam.h5'
with open('./ver_0.3/backend/MLTraining/training/news/tokenizer_news.pickle', 'rb') as handle:
    tokenizer = pickle.load(handle)

analyzer = SentimentAnalyzer(model_path, tokenizer)

# Consume data and save it in elasticsearch datastore.
for message in consumer:
    try:
        msg_content = message.value
        desc = msg_content["description"]
        publish_at = msg_content["date_time"]
        publish_date = parser.parse(publish_at)
        publish_date_next_day = publish_date + timedelta(days=1)
        price_day = fetch_bitcoin_prices(publish_date)
        price_next_day = fetch_bitcoin_prices(publish_date + timedelta(days=1))

        # Calculate change in price and adjusted probability
        if price_day is not None and price_next_day is not None:
            price_change = price_next_day - price_day
            print(f"price_change: {price_change}")
            if price_change > 0 :
                adjusted_probability = price_change / highest * 1.25
                print(f"{publish_date}: {price_day}")
                print(f"{publish_date_next_day}: {price_next_day}")
                print("Adjusted Probability:", adjusted_probability)
            elif price_change < 0 :
                adjusted_probability = price_change / lowest * -1.25
                print(f"{publish_date}: {price_day}")
                print(f"{publish_date_next_day}: {price_next_day}")
                print("Adjusted Probability:", adjusted_probability)
        else:
            adjusted_probability = default_probability

        result = analyzer.sentiment_analysis(desc, probability=adjusted_probability)
        msg_content["sentiment"] = result["sentiment"]
        res = es.index(index="news-data-2", body=msg_content)
    except Exception as e:
        print(f"Error processing message: {e}")
