from sentence_transformers import SentenceTransformer
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import pickle as pkl

df = pd.read_csv("../files/preprocessed_articles.csv")

paragraphs = list(df["text"])

model_bert = SentenceTransformer('bert-base-nli-max-tokens')

embedding_bert = np.array(model_bert.encode(paragraphs, show_progress_bar=True))

#Bert embeddings are shape of 768
print("Bert Embedding shape", embedding_bert.shape)
print("Bert Embedding sample", embedding_bert[0][0:50])

with open('../files/embeddings.pickle.pkl', 'wb') as f:
    pkl.dump(embedding_bert, f)