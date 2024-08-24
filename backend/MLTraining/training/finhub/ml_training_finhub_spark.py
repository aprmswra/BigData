import numpy as np
import datetime
import pandas as pd
import joblib
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, to_timestamp, percent_rank, monotonically_increasing_id
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error 
from keras.models import Sequential
from keras.layers import Dense, LSTM, Bidirectional
from keras.callbacks import EarlyStopping
import pyarrow as pa
import pickle
import json

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

# Define a function to write model paths to a JSON file, used for tracking model deployment.
def write_paths_to_file(log_filename, model_path, scaler_path, current_date, RMSE, num_train_samples):
    details = {
        "date": current_date,
        "model_path": model_path,
        "scaler_path": scaler_path,
        "RMSE": RMSE,
        "train_samples": num_train_samples
    }
    
    print("writing...")
    
    with open(log_filename, 'w') as file:
        json.dump(details, file, indent=4) 
    
    print("writing paths success")

# Set paths for the scaler and model using the current date, and log filename
current_date = datetime.datetime.now().strftime("%Y-%m-%d")
scaler_path = f"./ver_0.3/backend/MLTraining/training/finhub/scaler.pkl"
model_path = f"./ver_0.3/backend/MLTraining/training/finhub/model_lstm_{current_date}.h5"
log_filename = "./ver_0.3/backend/MLTraining/logs/training.log"

# Connect to HDFS and read a CSV file into a pandas DataFrame
hdfs = pa.hdfs.connect()
spark = SparkSession.builder.appName("BTCUSDProcessing").getOrCreate()
with hdfs.open('hdfs://127.0.0.1:9000/finhub/btcusdt_data_timeframe_1d.csv', 'rb') as f:
    df = pd.read_csv(f)

df['Datetime'] = pd.to_datetime(df['Date'])
df['Datetime'] = df['Datetime'].dt.tz_localize(None)

df.set_index('Datetime', inplace=True)
df_close = pd.DataFrame(df['Close'])

# Scale the 'Close' prices to be between 0 and 1
scaler = MinMaxScaler(feature_range=(0, 1))
df_close['ScaledClose'] = scaler.fit_transform(df_close[['Close']])
with open(scaler_path, 'wb') as file:
    pickle.dump(scaler, file)

final_spark_df = spark.createDataFrame(df_close.reset_index())
df_final = final_spark_df.select("Datetime", "Close", "ScaledClose")
df_with_rank = df_final.withColumn("rank", percent_rank().over(Window.orderBy("Datetime")))

# Split the data into training and testing sets based on the rank
train_df = df_with_rank.where("rank <= .75").drop("rank")
num_train_samples = train_df.count()
test_df = df_with_rank.where("rank > .75").drop("rank")
train_df = train_df.withColumn("id", monotonically_increasing_id())
train_rdd = train_df.rdd
test_df = test_df.withColumn("id", monotonically_increasing_id())
test_rdd = test_df.rdd

# Prepare the data using the windowing function defined earlier
x_train, y_train = windowing(train_rdd.collect())
x_train, y_train = np.array(x_train), np.array(y_train)
x_train = np.reshape(x_train, (x_train.shape[0], x_train.shape[1], 1))
x_test, y_test = windowing(test_rdd.collect())
x_test, y_test = np.array(x_test), np.array(y_test)
x_test = np.reshape(x_test, (x_test.shape[0], x_test.shape[1], 1))

#Train the model
model = Sequential([
    LSTM(50, return_sequences=True, input_shape=(x_train.shape[1], 1)), 
    LSTM(64, return_sequences=False),  
    Dense(32),  
    Dense(16),  
    Dense(1)   
])
model.compile(optimizer='adam', loss='mse', metrics=["mean_absolute_error"])
callbacks = [EarlyStopping(monitor='loss', patience=10, restore_best_weights=True)]
history = model.fit(x_train, y_train, epochs=100, batch_size=32, callbacks=callbacks, verbose=0)
model.save(model_path)

# Evaluate the model's performance using RMSE
predictions = model.predict(x_test)
predictions = predictions.squeeze()
predictions = scaler.inverse_transform([predictions])
y_test = scaler.inverse_transform([y_test])
RMSE = np.sqrt(np.mean( (y_test - predictions)**2 )).round(2)

with open(log_filename, "a") as file:
    file.write(f"Date: {current_date}, Model: {model_path}, Scaler: {scaler_path}, RMSE: {RMSE}\n")

# Output to verify file operations
print(f"Model saved as: {model_path}")
print(f"Scaler saved as: {scaler_path}")
print(f"Log updated in: {log_filename}")
