from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.preprocessing.sequence import pad_sequences
import tensorflow as tf
from nltk import pos_tag, word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import nltk
import numpy as np

# Ensure NLTK resources are downloaded
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
nltk.download('wordnet')
nltk.download('stopwords')

# Initialize NLTK tools
lemmatizer = WordNetLemmatizer()
stop_words = set(stopwords.words('english'))

def get_simple_pos(tag):
    """Simplify part-of-speech tags."""
    if tag.startswith('J'):
        return 'a'
    elif tag.startswith('V'):
        return 'v'
    elif tag.startswith('N'):
        return 'n'
    elif tag.startswith('R'):
        return 'r'
    else:
        return 'n'

def clean_review(text):
    """Clean and lemmatize input text."""
    clean_text = []
    for w in word_tokenize(text):
        if w.lower() not in stop_words:
            pos = pos_tag([w])
            new_w = lemmatizer.lemmatize(w, get_simple_pos(pos[0][1]))
            clean_text.append(new_w)
    return clean_text

def join_text(text):
    """Join cleaned text."""
    return " ".join(text)

class SentimentAnalyzer:
    def __init__(self, model_path, tokenizer):
        """Initialize SentimentAnalyzer with a trained model and a fitted tokenizer."""
        self.model = tf.keras.models.load_model(model_path)
        self.tokenizer = tokenizer

    def preprocess_text(self, text):
        """Preprocess text for model prediction."""
        cleaned_text = clean_review(text)
        joined_text = join_text(cleaned_text)
        sequences = self.tokenizer.texts_to_sequences([joined_text])
        padded_sequences = pad_sequences(sequences, maxlen=4000)  # Adjust maxlen according to your model's input
        return padded_sequences

    def adjustment_labels(self, data, probability):
        """Adjust labels based on the probability."""
        chance_of_change = abs(probability)
        if probability > 0:
            for i in range(len(data)):
                if np.random.rand() < chance_of_change:  
                    if data[i][0] == 1 or data[i][1] == 1: 
                        data[i] = [0, 0, 1]  
        else:
            for i in range(len(data)):
                if np.random.rand() < chance_of_change:  
                    if data[i][1] == 1 or data[i][2] == 1:  
                        data[i] = [1, 0, 0] 
        return data

    def sentiment_analysis(self, text, probability=0):
        """Predict and adjust sentiment of the input text based on probability."""
        preprocessed_text = self.preprocess_text(text)
        prediction = self.model.predict(preprocessed_text)
        
        # Adjust predictions based on the probability parameter
        adjusted_prediction = self.adjustment_labels(prediction, probability)
        
        # Determine sentiment from adjusted prediction
        if adjusted_prediction[0][0] == 1.0:  # Negative
            sentiment = "Negative"
        elif adjusted_prediction[0][1] == 1.0:  # Neutral
            sentiment = "Neutral"
        else:  # Positive
            sentiment = "Positive"

        return {"sentiment": sentiment}



