from flask import Flask, request, jsonify
from elasticsearch import Elasticsearch
from datetime import datetime
import json
import threading
from flask_cors import CORS
import time

# Initialize Flask application and enable CORS
app = Flask(__name__)
CORS(app)

# Connect to Elasticsearch
es = Elasticsearch("http://localhost:9200")

# Define route for retrieving news data
@app.route('/api/news', methods=['GET'])
def get_data():
    # Log the receipt of request
    print("Received request")
    # Extract start and end dates from the query parameters
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')
    print(f"Start Date: {start_date}, End Date: {end_date}")
    
    # Define the expected datetime format
    datetime_format = '%Y-%m-%dT%H:%M'
    try:
        # Validate the provided dates
        if start_date:
            datetime.strptime(start_date, datetime_format)
        if end_date:
            datetime.strptime(end_date, datetime_format)
    except ValueError:
        # Return error if the date format is incorrect
        return jsonify({"error": "Incorrect datetime format, should be YYYY-MM-DDTHH:MM"}), 400
    print("Date validation passed")    

    # Calculate the duration between start and end dates
    start_datetime = datetime.strptime(start_date, datetime_format)
    end_datetime = datetime.strptime(end_date, datetime_format)
    difference = end_datetime - start_datetime
    difference_in_days = difference.days + (difference.seconds / (3600 * 24))
    
    # Determine the size parameter for Elasticsearch queries
    size_minute = int(difference_in_days * 1440)
    if size_minute > 9999:
        size_minute = 10000

    # Prepare Elasticsearch query for news data
    print("Running Elasticsearch query")
    news_query = {
        "query": {
            "range": {
                "retrieved_at": {
                    "gte": start_date,
                    "lte": end_date,
                    "format": "yyyy-MM-dd'T'HH:mm"
                }
            }
        },
        "aggs": {
            "sentiment_counts": {
                "terms": {"field": "sentiment.keyword"}
            }
        },
        "size": size_minute  
    }

    # Prepare Elasticsearch query for financial data
    finhub_query = {
        "size": 10, 
        "query": {
            "range": {
                "timestamp": {
                    "gte": start_date,
                    "lte": end_date,
                    "format": "yyyy-MM-dd'T'HH:mm"
                }
            }
        },
        "aggs": {
            "volume_sum": {
                "sum": {
                    "field": "volume"  
                }
            }
        },
    }

    # Execute queries and process responses
    print("Elasticsearch query completed")
    news_response = es.search(index="news-data-2", body=news_query)
    sentiments = news_response.get('aggregations', {}).get('sentiment_counts', {}).get('buckets', [])
    total_news = news_response.get('hits', {}).get('total', {}).get('value', 0)
    news_items = news_response.get('hits', {}).get('hits', [])

    # Format news items for the response
    formatted_news = [{
        "id": item.get('_id', 'Unknown ID'),
        "index": item.get('_index', 'No index'),
        "score": item.get('_score', 0),
        "date_time": item['_source'].get('date_time', 'No date'),
        "description": item['_source'].get('description', 'No description'),
        "sentiment": item['_source'].get('sentiment', 'No sentiment'),
        "source": item['_source'].get('source', 'No source'),
        "title": item['_source'].get('title', 'No title')
    } for item in news_items]
    print("News RESTAPI success")

    # Execute and process financial data query
    finhub_response = es.search(index="new-finhub-data-2", body=finhub_query)
    print(finhub_response)
    formatted_finhubs_list = []
    finhub_hits = finhub_response.get('hits', {}).get('hits', [])
    total_volume = finhub_response.get('aggregations', {}).get('volume_sum', {}).get('value', 0)

    # Format financial items for the response
    for hit in finhub_hits:
        formatted_finhubs_list.append({
            "timestamp": hit['_source'].get("timestamp", "No timestamp"),
            "price": hit['_source'].get("price", "No price"),
            "prediction": hit['_source'].get("predicted_price", "No prediction"),
            "timestamp_prediction": hit['_source'].get("prediction_time", "No prediction time"),
            "volume": hit['_source'].get("volume", "No volume"),
            "total_volume": total_volume,
            "symbol": hit['_source'].get("symbol", "No symbol")
        })
    print("Finhub RESTAPI success")

    # Compile and send the final response
    response = {
        "total_news": total_news,
        "sentiments": sentiments,
        "news": formatted_news,
        "latest_financial": formatted_finhubs_list,
    }
    print("Sending response")
    return jsonify(response)

# Run the Flask application in debug mode
if __name__ == '__main__':
    app.run(debug=True)
