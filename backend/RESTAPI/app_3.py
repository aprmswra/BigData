from flask import Flask, request, jsonify
from elasticsearch import Elasticsearch
from datetime import datetime
from flask_cors import CORS

# Initialize Flask application and enable CORS
app = Flask(__name__)
CORS(app)

# Connect to Elasticsearch
es = Elasticsearch("http://localhost:9200")

@app.route('/api/news', methods=['GET'])
def get_data():
    # Log the receipt of request
    print("Received request")
    
    # Extract start and end dates from the query parameters
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')
    print(f"Start Date: {start_date}, End Date: {end_date}")
    
    # Define the expected datetime format
    datetime_format = '%Y-%m-%d'
    try:
        # Validate the provided dates
        if start_date:
            datetime.strptime(start_date, datetime_format)
        if end_date:
            datetime.strptime(end_date, datetime_format)
    except ValueError:
        # Return error if the date format is incorrect
        return jsonify({"error": "Incorrect datetime format, should be YYYY-MM-DD"}), 400
    
    print("Date validation passed")    

    # Prepare Elasticsearch query for news data
    print("Running Elasticsearch query for news data")
    news_query = {
        "query": {
            "range": {
                "date_time": {
                    "gte": start_date,
                    "lte": end_date,
                    "format": "yyyy-MM-dd"
                }
            }
        },
        "aggs": {
            "sentiment_counts": {
                "terms": {"field": "sentiment.keyword"}
            }
        },
        "size": 1000  # Adjust size if needed
    }

    # Prepare Elasticsearch query for financial data
    print("Running Elasticsearch query for financial data")
    daily_query = {
      "size": 0, 
      "query": {
        "range": {
          "timestamp": {
            "gte": start_date,
            "lte": end_date,
            "format": "yyyy-MM-dd"
          }
        }
      },
      "aggs": {
        "by_day": {
          "date_histogram": {
            "field": "timestamp",
            "calendar_interval": "day",
            "format": "yyyy-MM-dd"
          },
          "aggs": {
            "latest_record": {
              "top_hits": {
                "sort": [
                  {
                    "timestamp": {
                      "order": "desc"
                    }
                  }
                ],
                "_source": {
                  "includes": ["predicted_price", "price", "timestamp", "prediction_time"]
                },
                "size": 1
              }
            },
            "volume_sum": {
              "sum": {
                "field": "volume"
              }
            }
          }
        }
      }
    }

    try:
        # Execute queries and process responses
        print("Executing Elasticsearch query for news data")
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
        print(formatted_news)

        # Execute and process financial data query
        print("Executing Elasticsearch query for financial data")
        financial_response = es.search(index="new-finhub-data-6", body=daily_query)

        formatted_financial_data = []
        buckets = financial_response['aggregations']['by_day']['buckets']
        for bucket in buckets:
            if bucket['latest_record']['hits']['total']['value'] > 0:
                latest_record = bucket['latest_record']['hits']['hits'][0]['_source']
                predicted_price = latest_record.get('predicted_price', None)
                prediction_time = latest_record.get('prediction_time', None)
                price = latest_record.get('price', None)
                timestamp = latest_record.get('timestamp', None)
                total_volume = bucket['volume_sum']['value']
                formatted_financial_data.append({
                    "timestamp": timestamp,
                    "predicted_price": predicted_price,
                    "price": price,
                    "prediction_time":prediction_time,
                    "total_volume": total_volume
                })
        print("Financial RESTAPI success")
        # Compile and send the final response
        response = {
            "total_news": total_news,
            "sentiments": sentiments,
            "news": formatted_news,
            "latest_financial": formatted_financial_data,
        }
        print("Sending response")
        return jsonify(response)
        print(response)

    except Exception as e:
        print(f"Error occurred: {e}")
        return jsonify({"error": "An error occurred while processing the request"}), 500

# Run the Flask application in debug mode
if __name__ == '__main__':
    app.run(debug=True)
