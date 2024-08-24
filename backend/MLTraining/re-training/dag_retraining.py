from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from hdfs import InsecureClient
import json
import pandas as pd
from pyspark.sql import SparkSession, Window
import datetime
import joblib
import matplotlib.pyplot as plt  

from pyspark.sql.functions import col, to_timestamp, percent_rank, monotonically_increasing_id
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline

from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error  

from keras.models import Sequential
from keras.layers import Dense, LSTM, Bidirectional
from keras.callbacks import EarlyStopping
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, unix_timestamp
import numpy as np
from pyspark.sql import Window
from pyspark.sql.functions import col, to_timestamp, unix_timestamp, lag, sum as _sum, when, to_date, hour, minute, month, dayofmonth, year, concat_ws

import json
import pickle
import re
import os

log_filename = "./ver_0.3/backend/MLTraining/logs/training.log"

# Define a function to write model paths to a JSON file, used for tracking model deployment.
def write_paths_to_file(model_path):
    paths = {
        "model_path": model_path
    }
    print("writing...")
    with open('./ver_0.3/backend/MLTraining/logs/paths.json', 'w') as file:
        json.dump(paths, file)
    print("writing paths success")

# Define a function to check if data in a DataFrame forms a continuous sequence based on time intervals.
def check_sequence(df, sequence_days=90):
    if df.rdd.isEmpty():
        print("DataFrame is empty.")
        return False

    # Order the DataFrame by timestamp
    sorted_df = df.orderBy("timestamp")

    # Define a window specification ordered by timestamp
    windowSpec = Window.orderBy("timestamp")

    # Calculate differences in days between consecutive timestamps
    sorted_df = sorted_df.withColumn("previous_timestamp", lag("timestamp", 1).over(windowSpec))
    sorted_df = sorted_df.withColumn("diff_days",
                                     when(col("previous_timestamp").isNull(), None)
                                     .otherwise((unix_timestamp("timestamp") - unix_timestamp("previous_timestamp")) / (60 * 60 * 24)))

    # Flag to increment groups when there is no continuity
    sorted_df = sorted_df.withColumn("increment",
                                     when(col("diff_days") != 1, 1).otherwise(0))

    # Sum increments to form groups
    sorted_df = sorted_df.withColumn("group",
                                     _sum("increment").over(windowSpec))

    # Count the number of days in each consecutive group
    group_counts = sorted_df.groupBy("group").count()

    # Filter for groups with at least the required number of consecutive days
    valid_groups = group_counts.filter(col("count") >= sequence_days).collect()

    if valid_groups:
        valid_group_ids_list = [row['group'] for row in valid_groups]

        # Extract rows that belong to valid groups
        consecutive_days_df = sorted_df.filter(sorted_df['group'].isin(valid_group_ids_list))

        # Drop helper columns if they are no longer needed
        final_df = consecutive_days_df.drop("previous_timestamp", "diff_days", "increment", "group")

        return final_df
    else:
        print("No valid sequence found.")
        return False

# Windowing function with RDD
def windowing(data, window_size=7):
    data_sorted = sorted(data, key=lambda row: row['id'])
    x_train, y_train = [], []
    for i in range(window_size, len(data_sorted)):
        x_window = [row['ScaledClose'] for row in data_sorted[i-window_size:i]]
        y_value = data_sorted[i]['ScaledClose']
        x_train.append(x_window)
        y_train.append(y_value)
    return x_train, y_train


# Airflow task to train a machine learning model using Spark and other libraries.
def train_model(**context):
    spark = SparkSession.builder.appName("Streaming_Data_Training_Finhub").getOrCreate()
    hdfs_client = InsecureClient('http://localhost:9870', user='bigdata')  
    path = '/finhub/finhub_5.json'

    data_list = []

    with hdfs_client.read(path, encoding='utf-8') as reader:
        for line in reader:
            line = line.strip()
            if line:  # Memastikan baris tidak kosong
                try:
                    data = json.loads(line)
                    data_list.append(data)
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON: {e}")
                    continue

    df = pd.DataFrame(data_list)

    
    sdf = spark.createDataFrame(df)
    sdf = sdf.withColumn("timestamp", to_timestamp(col("timestamp")))
    sdf = sdf.withColumn("date", to_date("timestamp")) \
             .withColumn("year", year("timestamp").cast("string")) \
             .withColumn("month", month("timestamp").cast("string")) \
             .withColumn("day", dayofmonth("timestamp").cast("string")) \
             .withColumn("timestamp", concat_ws("-", col("year"), col("month"), col("day")))

    checked_sdf = check_sequence(df = sdf)
    print(checked_sdf)

    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    scaler_path = f"./ver_0.3/backend/MLTraining/training/finhub/scaler.pkl"
    model_path = f"./ver_0.3/backend/MLTraining/training/finhub/model_lstm_realtime_streaming_retraining_newest_{current_date}.h5"
    log_filename = "./ver_0.3/backend/MLTraining/logs/training.log"

    assembler = VectorAssembler(inputCols=["price"], outputCol="CloseVec")
    df_vector = assembler.transform(checked_sdf)  # Ensure your sdf has a 'price' column

    # Mengonversi Spark DataFrame ke Pandas DataFrame untuk proses scaling
    pandas_df = df_vector.select("timestamp", "price", "CloseVec").toPandas()
    pandas_df['CloseVec'] = pandas_df['CloseVec'].apply(lambda x: x[0])
    pandas_df['price'] = pandas_df['price'].astype(float)
    # scalled data
    with open(scaler_path, 'rb') as file:
        scaler = pickle.load(file)
    pandas_df['ScaledClose'] = scaler.fit_transform(pandas_df[['CloseVec']])

    # Mengonversi kembali ke Spark DataFrame
    final_spark_df = spark.createDataFrame(pandas_df)
    
    # Memilih kolom yang relevan untuk tampilan akhir
    df_final = final_spark_df.select("timestamp", "price", "ScaledClose")

    # Menghitung persentil rangking berdasarkan 'Datetime' untuk membagi data
    df_with_rank = df_final.withColumn("rank", percent_rank().over(Window.orderBy("timestamp")))
    train_df = df_with_rank.where("rank <= .75").drop("rank")
    test_df = df_with_rank.where("rank > .75").drop("rank")

    # Menambahkan ID unik untuk setiap baris untuk memudahkan pemrosesan RDD
    train_df = train_df.withColumn("id", monotonically_increasing_id())
    train_rdd = train_df.rdd
    test_df = test_df.withColumn("id", monotonically_increasing_id())
    test_rdd = test_df.rdd

    # Mengaplikasikan fungsi windowing
    x_train, y_train = windowing(train_rdd.collect())
    x_train, y_train = np.array(x_train), np.array(y_train)
    x_train = np.reshape(x_train, (x_train.shape[0], x_train.shape[1], 1))

    x_test, y_test = windowing(test_rdd.collect())
    x_test, y_test = np.array(x_test), np.array(y_test)
    x_test = np.reshape(x_test, (x_test.shape[0], x_test.shape[1], 1))

    # Define the LSTM model architecture
    model = Sequential([
        Bidirectional(LSTM(50, return_sequences=True, input_shape=(x_train.shape[1], 1))),
        Bidirectional(LSTM(64, return_sequences=True)),
        Bidirectional(LSTM(64, return_sequences=False)),
        Dense(32, activation='relu'),
        Dense(16, activation='relu'),
        Dense(1)
    ])

    # Compile the model
    model.compile(optimizer='adam', loss='mse', metrics=["mean_absolute_error"])

    callbacks = [EarlyStopping(monitor='loss', patience=10, restore_best_weights=True)]

    history = model.fit(x_train, y_train, epochs=100, batch_size=32, callbacks=callbacks, verbose=0)

    model.save(model_path)

    predictions = model.predict(x_test)
    predictions = predictions.squeeze()
    predictions = scaler.inverse_transform([predictions])

    y_test = scaler.inverse_transform([y_test])

    RMSE = np.sqrt(np.mean( (y_test - predictions)**2 )).round(2)

    with open(log_filename, "a") as file:
        file.write(f"Date: {current_date}, Model: {model_path}, Scaler: {scaler_path}, RMSE: {RMSE}\n")

    context['task_instance'].xcom_push(key='rmse', value=RMSE)
    context['task_instance'].xcom_push(key='model_path', value=model_path)
    context['task_instance'].xcom_push(key='scaler_path', value=scaler_path)

# Airflow task to evaluate the new model against previous models and decide whether to update the production model.
def check_and_change_model(**context):
    new_model_rmse = context['task_instance'].xcom_pull(task_ids='train_model', key='rmse')

    best_model = None
    best_rmse = float('inf')
    best_model_info = {}

    with open(log_filename, 'r') as log_file:
        for line in log_file:
            match = re.search(r'RMSE: ([\d.]+)', line)
            if match:
                rmse = float(match.group(1))
                if rmse < best_rmse:
                    best_rmse = rmse
                    best_model_info = {
                        'model_path': re.search(r'Model: (.+?),', line).group(1),
                        'scaler_path': re.search(r'Scaler: (.+?),', line).group(1),
                        'rmse': rmse
                    }
    
    # Compare the RMSE of the best model with the new model
    if new_model_rmse < best_rmse:
        print(f"New model is better. RMSE: {new_model_rmse} < {best_rmse}")
        print(f"New model path: {context['task_instance'].xcom_pull(task_ids='train_model', key='model_path')}")
        model_path = context['task_instance'].xcom_pull(task_ids='train_model', key='model_path')
        write_paths_to_file(model_path)
    else:
        print(f"Current model is still the best. RMSE: {new_model_rmse} >= {best_rmse}")
        print(f"Best model path: {best_model_info['model_path']}")
        model_path = best_model_info['model_path']
        write_paths_to_file(model_path)

# Set default arguments for the DAG, including start date, email configuration, retry policy, etc.
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['23522002@std.stei.itb.ac.id'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG, set its schedule, description, and default arguments.
dag = DAG(
    'retraining_price_prediction_fixed',
    default_args=default_args,
    description='A DAG to train a crypto price prediction model monthly at midnight',
    schedule_interval='0 0 1 * *',   # 00:00 on the first day of each month
)

# Define tasks using PythonOperator to link Python functions to the DAG.
train_model_task = PythonOperator(
    task_id='train_model',
    python_callable=train_model,
    dag=dag,
)

check_and_change_model = PythonOperator(
    task_id='check_and_change_model',
    python_callable=check_and_change_model,
    dag=dag,
)

train_model_task >> check_and_change_model
