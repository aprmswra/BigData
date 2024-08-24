from kafka import KafkaConsumer
from hdfs import InsecureClient
import json
from elasticsearch import Elasticsearch
from keras.models import load_model
from sklearn.preprocessing import MinMaxScaler
import pickle
from datetime import datetime, timedelta
import numpy as np
from hdfs.util import HdfsError

def get_prediction_for_next_period(model, last_prices, scaler):
    last_prices_scaled = scaler.transform(np.array(last_prices).reshape(-1, 1))
    last_prices_scaled = np.reshape(last_prices_scaled, (1, last_prices_scaled.shape[0], 1))
    predicted_scaled_price = model.predict(last_prices_scaled)
    predicted_price = scaler.inverse_transform(predicted_scaled_price)
    return predicted_price[0, 0]

def read_paths_from_file():
    try:
        with open('./ver_0.3/backend/MLTraining/logs/paths.json', 'r') as file:
            paths = json.load(file)
        return paths['model_path']
    except (FileNotFoundError, KeyError):
        return None  

model_path = read_paths_from_file()
scaler_path = "./ver_0.3/backend/MLTraining/training/finhub/scaler.pkl"
print(f"model_path: {model_path}")
print(f"scaler_path: {scaler_path}")

if not model_path or not scaler_path:
    print("Model or scaler path could not be found.")
    exit(1)  

model = load_model(model_path)
with open(scaler_path, 'rb') as file:
    scaler = pickle.load(file)

print(f"scaler: {type(scaler)}")

es = Elasticsearch("http://localhost:9200")

consumer = KafkaConsumer(
    'finhub',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

hdfs_client = InsecureClient('http://localhost:9870', user='bigdata')

messages_list = [] 
last_7_prices = []

def append_to_hdfs_file(path, data):
    bytes_data = json.dumps(data).encode('utf-8')
    try:
        # Check if the file exists by trying to read it
        hdfs_client.status(path)
        # If the file exists, append to it
        with hdfs_client.write(path, append=True) as writer:
            writer.write(bytes_data + b'\n')
    except HdfsError:
        # If the file does not exist, create it and write data to it
        with hdfs_client.write(path, overwrite=True) as writer:
            writer.write(bytes_data + b'\n')

def get_last_6_prices():
    end_time = datetime.now().replace(hour=23, minute=0, second=0, microsecond=0)
    last_6_days = [end_time - timedelta(days=i) for i in range(1, 9)]
    last_6_prices = []

    for day in last_6_days:
        timestamp = day.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        try:
            result = es.search(index='new-finhub-data-3', body={
                'query': {
                    'term': {
                        'timestamp': timestamp
                    }
                }
            })
            if result['hits']['hits']:
                last_6_prices.append(result['hits']['hits'][0]['_source']['price'])
        except Exception as e:
            print(f"Error fetching price for {timestamp}: {e}")
            return None
    
    return last_6_prices 

last_7_prices = get_last_6_prices()
print(last_7_prices)
print(f"before_append: data length: {len(last_7_prices)}, list: {last_7_prices}")
hdfs_file_path = '/finhub/finhub_3.json'

for message in consumer:
    message_data = message.value
        
    if all(key in message_data for key in ['price', 'volume', 'symbol', '@timestamp']):
        current_price = message_data['price']
        volume = message_data['volume']
        symbol = message_data['symbol']
        # Adjust the timestamp for the timezone difference
        timestamp_str = message_data['@timestamp'].replace('Z', '')
        current_time = datetime.fromisoformat(timestamp_str)
        current_time += timedelta(hours=7)
        last_7_prices.append(current_price)
        last_7_prices = last_7_prices[-7:]
    else:
        print("Warning: Missing required keys in message. Skipping...")
        continue

    # Get last 7 prices from previous 7 days at 23:00:00
    if len(last_7_prices)==7:
        predicted_price = get_prediction_for_next_period(model, last_7_prices, scaler)
        prediction_time = current_time + timedelta(days=7)

        message_to_index = {
            'timestamp': current_time.isoformat(),
            'price': float(current_price),
            'prediction_time': prediction_time.isoformat(),
            'predicted_price': float(predicted_price),
            'volume':float(volume),
            'symbol':str(symbol)
            }

        # Append to HDFS
        append_to_hdfs_file(hdfs_file_path, message_to_index)
        res = es.index(index="new-finhub-data-3", body=message_to_index)
        entry = {"index": res, "message": message_to_index}
        messages_list.append(entry)
        print(f"Indexed message: {res['_id']}, close_price = {current_price}")
    else:
        print("Insufficient data for prediction, skipping...")
