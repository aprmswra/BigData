import websocket
import datetime
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

def get_api_key(filepath, key_name):
    with open(filepath, 'r') as file: 
        for line in file:  
            if line.startswith(key_name):  
                return line.split(',')[1].strip()  
    return None  

daily_data = {}

def on_message(ws, message):
    message_json = json.loads(message)
    message_json["@timestamp"] = datetime.datetime.utcnow().isoformat()
    # Extract date and data
    date = message_json["@timestamp"].split("T")[0]
    
    # Process each transaction in the message
    for transaction in message_json['data']:
        price = transaction['p']
        volume = transaction['v']
        symbol = transaction['s']  # Extract symbol

        # Check if the date and symbol combination is already tracked
        if (date, symbol) not in daily_data:
            daily_data[(date, symbol)] = {"symbol": symbol, "price": price, "volume": volume}
        else:
            daily_data[(date, symbol)]["volume"] += volume
            daily_data[(date, symbol)]["price"] = price  # Update to the last price of the day
    
    # Print the current state of daily_data
    for key, value in daily_data.items():
        print(f"Total data for {key[0]} and symbol {key[1]}: {value}")

    # Send data to Kafka at the start of each hour
    current_time = datetime.datetime.utcnow()
    if current_time.minute == 4:
        for (date, symbol), data in daily_data.items():
            data["@timestamp"] = current_time.strftime('%Y-%m-%dT%H:%M:%SZ')
            producer.send('finhub', value=data)
            print(f"Sent data: {data}")
        daily_data.clear()
        time.sleep(60)  # Sleep to avoid sending multiple times during the same minute


    print("Message sent to Kafka topic")  

def on_error(ws, error):
    print(error) 

def on_close(ws):
    print("### closed ###")  

def on_open(ws):
    ws.send('{"type":"subscribe","symbol":"BINANCE:BTCUSDT"}')

api_key = get_api_key('./ver_0.3/backend/kafka/api_key/api_key.txt', 'api_finhub')

if __name__ == "__main__":
    websocket.enableTrace(True) 
    ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={api_key}",
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()
