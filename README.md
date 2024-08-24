Automated Streaming Cryptocurrency Data Analysis

![BigDataArchitecture drawio](https://github.com/user-attachments/assets/09cff28a-01bb-4e19-a737-43355f1db30d)

## Components

### 1. Data Streams
- **Finhub.io**: Provides real-time cryptocurrency price data.
- **News API**: Provides real-time news data relevant to cryptocurrency markets.

### 2. Apache Kafka
- **Crypto Topic**: Manages the streaming of cryptocurrency price data.
- **News Topic**: Manages the streaming of news data.

### 3. Zookeeper
- Manages the state of the Kafka cluster, ensuring high availability and fault tolerance.

### 4. Consumers
- **Consumer_finhub**: Reads data from the Crypto Topic.
- **Consumer_news**: Reads data from the News Topic.

### 5. Streaming Data Storage
- **Streaming Data Crypto**: Intermediate storage for streaming cryptocurrency data.
- **Streaming Data News**: Intermediate storage for streaming news data.

### 6. Machine Learning Predictions
- **Price Prediction**: Predicts future cryptocurrency prices using a machine learning model.
- **Sentiment Analysis**: Analyzes news data to gauge market sentiment.

### 7. HDFS (Hadoop Distributed File System)
- Stores training data for the machine learning models.

### 8. Apache Spark
- Processes data from HDFS for both price prediction and sentiment analysis.

### 9. Elasticsearch
- Stores processed data, allowing for quick retrieval and search capabilities.

### 10. REST API
- Serves as the interface between the backend data and the user-facing dashboard.

### 11. Dashboard (UI)
- Presents the processed data (e.g., price predictions, sentiment analysis) to the end-user in a clear and interactive manner.

### 12. Training ML and Retraining with Apache Airflow
- Trains and periodically retrains the machine learning models to ensure accuracy.

## Getting Started

### Prerequisites
- Apache Kafka
- Apache Zookeeper
- Apache Spark
- HDFS
- Elasticsearch
- Apache Airflow

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/aprmswra/BigData.git
