import pandas as pd
import numpy as np
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.tag import pos_tag
nltk.download('stopwords')
nltk.download('punkt')

def split_main_text(main_text):
    if pd.notna(main_text):
        main_text_list = main_text.split('\a')
        return main_text_list
    else:
        return []
    
def concat_cols(line):
    try:
        text = line["title"] + " " + line["date"] + " " + line["category"] + " " + line["headers"] + " " + line["main_text_splitted"]
    except:
        text = line["title"] + " " + line["date"] + " " + line["category"] + " " + line["main_text_splitted"]
    return text

df = pd.read_csv("../files/articles_V2.csv")
df['main_text_splitted'] = df['main_text'].apply(lambda x: split_main_text(x))
df = df.drop(['summary', 'main_text'], axis=1)
df = df[df['category'] == "Climate and Environment"]
df = df.reset_index(drop=True)
df = df.explode('main_text_splitted')
df.reset_index(drop=True, inplace=True)
ind = df[df["main_text_splitted"].isnull()].index
df = df.drop(ind, axis=0)
df.reset_index(drop=True, inplace=True)
df["main_text_len"] = df["main_text_splitted"].apply(lambda x: len(x))
df.groupby(by="main_text_len").count()["main_text_splitted"].plot()
df = df[(df["main_text_len"]) > 100]
df.reset_index(drop=True, inplace=True)
df.groupby(by="main_text_len").count()["main_text_splitted"].plot()
df["text"] = df.apply(lambda x: concat_cols(x), axis=1)
df = df.drop(["title", "date","category","headers", "main_text_splitted", "main_text_len", "link"], axis=1)
df.reset_index(drop=True, inplace=True)

df.to_csv('../files/preprocessed_articles.csv', index=False)