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

def cosine(u, v):
    return np.dot(u, v) / (np.linalg.norm(u) * np.linalg.norm(v))

df = pd.read_csv("../files/preprocessed_articles.csv")
sents = list(df["text"])
sent_tokens = [cleantxt(sent) for sent in sents]
tagged_data = [TaggedDocument(d, [i]) for i, d in enumerate(sent_tokens)]
## Train doc2vec model
model = Doc2Vec(tagged_data,
                 vector_size = 20,
                 window = 2,
                 min_count = 1,
                 epochs = 100)

'''
vector_size = Dimensionality of the feature vectors.
window = The maximum distance between the current and predicted word within a sentence.
min_count = Ignores all words with total frequency lower than this.
alpha = The initial learning rate.
'''
embeddings_sent2vec = model.wv.vectors
embeddings_rows = model.wv.vectors.shape[0]
embeddings_cols = model.wv.vectors.shape[1]

with open(f'../files/embeddings_sent2vec_{embeddings_rows}_{embeddings_cols}.pkl', 'wb') as f:
    pkl.dump(embeddings_sent2vec, f)