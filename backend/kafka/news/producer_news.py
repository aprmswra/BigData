import json
from kafka import KafkaProducer
from datetime import datetime, date, timedelta
import requests
import time

# Function to retrieve an API key from a given file.
def get_api_key(filepath, key_name):
    with open(filepath, 'r') as file:  
        for line in file:  
            if line.startswith(key_name):  
                return line.split(',')[1].strip()  
    return None  

# Initialize a Kafka producer that sends data to a Kafka broker on localhost.
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

print("Connected to Kafka")

# Retrieve the API key for accessing news API.
api_key = get_api_key('./ver_0.3/kafka/api_key/api_key.txt', 'api_news')

# Format the URL for the API request.
url = ('https://newsapi.org/v2/everything?'
       'q=Bitcoin&'
       'from=2024-05-07&'
       'to=2024-05-19&'
       'sortBy=popularity&'
       'language=en&'
       'apiKey={0}').format(api_key)

# Send data to news_kafka_consumer.
def send_data():
    try:
        print("Connecting...")
        res = requests.get(url) 
        data = res.json()  
        articles = data["articles"]  

        for article in articles:
            news = {
                "title": article["title"],
                "description": article["description"],
                "date_time": article["publishedAt"],
                "source": article["source"]["name"],
                "retrieved_at": datetime.now().isoformat()  
            }
            print("Sending news...")
            producer.send('news-data-2', value=news)  
            time.sleep(15)  # Wait for 60 seconds before sending the next news.
    except Exception as e:
        print(e)  

def home():
    return {"message": "working fine"}  # Simple function to check the script status.

# Call the function to start sending data to consumer.
send_data()
