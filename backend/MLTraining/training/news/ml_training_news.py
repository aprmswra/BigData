import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.preprocessing import OneHotEncoder
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout
from tensorflow.keras.utils import to_categorical
from sklearn.metrics import f1_score
from hdfs import InsecureClient
from string import punctuation
from nltk.corpus import stopwords, wordnet
from nltk import pos_tag
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize,sent_tokenize
from tensorflow.keras.preprocessing.text import Tokenizer
import pickle
import os
import warnings
warnings.filterwarnings('ignore')


hdfs_client = InsecureClient('http://localhost:9870', user='bigdata')  

# Read the CSV file from HDFS
with hdfs_client.read('/news/news-data.csv', encoding='latin-1') as reader:
    data = pd.read_csv(reader, names=['sentiment', 'text'])

stop = set(stopwords.words('english'))
stop.update(punctuation)

# this function return the part of speech of a word.
def get_simple_pos(tag):
    if tag.startswith('J'):
        return wordnet.ADJ
    elif tag.startswith('V'):
        return wordnet.VERB
    elif tag.startswith('N'):
        return wordnet.NOUN
    elif tag.startswith('R'):
        return wordnet.ADV
    else:
        return wordnet.NOUN

# Function to clean our text.
lemmatizer = WordNetLemmatizer()
def clean_review(text):
    clean_text = []
    for w in word_tokenize(text):
        if w.lower() not in stop:
            pos = pos_tag([w])
            new_w = lemmatizer.lemmatize(w, pos=get_simple_pos(pos[0][1]))
            clean_text.append(new_w)
    return clean_text

def join_text(text):
    return " ".join(text)

data.text = data.text.apply(clean_review)
data.text = data.text.apply(join_text)

x_train,x_test,y_train,y_test = train_test_split(data.text,data.sentiment,test_size = 0.2 , random_state = 0)

count_vec = CountVectorizer(max_features=4000, ngram_range=(1,2), max_df=0.9, min_df=0)
x_train_features = count_vec.fit_transform(x_train).todense()
x_test_features = count_vec.transform(x_test).todense()

x_data = pd.concat([x_train, x_test])
tokenizer = Tokenizer(num_words=10000) 
tokenizer.fit_on_texts(x_data)
with open('./ver_0.3/backend/MLTraining/training/news/tokenizer_news.pickle', 'wb') as handle:
    pickle.dump(tokenizer, handle, protocol=pickle.HIGHEST_PROTOCOL)

y_train[y_train=='positive']=2
y_train[y_train=='neutral']=1
y_train[y_train=='negative']=0
y_test[y_test=='positive']=2
y_test[y_test=='neutral']=1
y_test[y_test=='negative']=0

encoder = OneHotEncoder()
y_train = to_categorical(y_train)
y_test = to_categorical(y_test)

def fit_model(optimizer):
    model = Sequential()
    
    # Definisikan arsitektur model
    model.add(Dense(units=512, activation='relu', input_dim=x_train_features.shape[1]))
    model.add(Dropout(0.2))
    model.add(Dense(units=256, activation='relu'))
    model.add(Dropout(0.2))
    model.add(Dense(units=3, activation='softmax'))
    
    # Kompilasi model
    model.compile(loss='categorical_crossentropy', optimizer=optimizer, metrics=['accuracy'])
    
    # Latih model
    history = model.fit(x_train_features, y_train, validation_data=(x_test_features, y_test), epochs=100, verbose=0)
       
    # Simpan model
    save_dir = './ver_0.3/backend/MLTraining/training/news'
    model_name = 'model_' + optimizer + '.h5'

    model_save_path = os.path.join(save_dir, model_name)
    model.save(model_save_path)
    print("Model saved at", model_save_path)
    
    return model

# Contoh pemanggilan fungsi
optimizers = ['adam']
for i, opt in enumerate(optimizers):
    model = fit_model(opt)


# Prediksi pada data validasi
predictions = model.predict(x_test_features)
y_pred = np.argmax(predictions, axis=1)
y_true = np.argmax(y_test, axis=1)

f1_macro = f1_score(y_true, y_pred, average='macro')