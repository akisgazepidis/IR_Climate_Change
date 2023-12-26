import string
import numpy as np
import pandas as pd
import re
import pickle as pkl

from gensim.models.doc2vec import Doc2Vec, TaggedDocument
import nltk
from nltk.corpus import stopwords
from nltk import word_tokenize, WordNetLemmatizer, PorterStemmer, pos_tag, RegexpParser
nltk.download('averaged_perceptron_tagger')
wn = WordNetLemmatizer()
ps = PorterStemmer()
stopwords = stopwords.words('english')

def cleantxt(text):
    text = text.replace('\r', ' ')
    text = text.replace('\n', ' ')
    text = text.replace('\t', ' ')
    text = text.strip()
    text = ''.join([wrd.lower() for wrd in text if wrd not in string.punctuation])
    tokens = re.split('\W+', text)
    tokens = [wrd for wrd in tokens if wrd not in stopwords]
    tokens = [wn.lemmatize(wrd) for wrd in tokens]
    return tokens

df = pd.read_csv("files/preprocessed_articles.csv")
with open(f'files/embeddings_sent2vec_23779_20.pkl', 'rb') as f:
    model = pkl.load(f)

embeddings_rows = model.wv.vectors.shape[0]
embeddings_cols = model.wv.vectors.shape[1]

new_sent = input("Enter your query: ")
test_doc = cleantxt(new_sent)
test_doc_vector = model.infer_vector(test_doc)
text_ind = model.dv.most_similar(positive = [test_doc_vector])[0][0]

print(df['text'][text_ind])










