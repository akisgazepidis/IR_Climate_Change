import pickle as pkl
from sentence_transformers import SentenceTransformer
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
import pandas as pd
from nltk import  pos_tag
import re

df = pd.read_csv("files/preprocessed_articles.csv")
with open('files/embeddings.pickle.pkl', 'rb') as f:
    embedding_bert = pkl.load(f)

query = input("Enter your query: ")

model_bert = SentenceTransformer('bert-base-nli-max-tokens')
query_embedding_bert = np.array(model_bert.encode([query]
                                                  ,show_progress_bar=True
                                ))
cosine_similarities = cosine_similarity(query_embedding_bert, embedding_bert)

df["cosine_similarities"] = pd.DataFrame(cosine_similarities).T
temp_df = df.sort_values(by= "cosine_similarities", ascending=False).head(10)
temp_df.reset_index(drop=True, inplace=True)

print(temp_df["text"][0])