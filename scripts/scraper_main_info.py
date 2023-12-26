import requests
from bs4 import BeautifulSoup
from tqdm.notebook import tqdm as tqdm # Explain loops

import json
import pandas as pd 
from time import sleep
from random2 import random
import numpy as np

with open("../files/url_list.txt", "r") as f:
    text = f.read()

urls = text.splitlines()

df= pd.read_csv('../files/articles.csv')

for url in urls[1284:1285]:
    print(url)
    try:
        response = requests.get(url, timeout= 25)
        sleep(random.uniform(3,4))
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')

            try:
                title = soup.find('h1').find('span', class_ = "field field--name-title field--type-string field--label-hidden").text
            except:
                title = np.nan

            try:    
                date = soup.find_all('span', class_ = "views-field views-field-field-news-date")[0].find('span', class_ = "field-content").find('time', class_ = "datetime").text
            except:
                date = np.nan

            try:
                category = soup.find_all('span', class_ = "views-field views-field-field-news-topics")[0].find('span', class_ = "field-content").text
            except:
                category = np.nan

            try:
                summary = soup.find_all('div', class_ = "views-field views-field-field-news-story-lead")[0].find('p').text
            except:
                summary = np.nan

            try:
                temp_text_div = soup.find_all('div', class_ = "clearfix text-formatted field field--name-field-text-column field--type-text-long field--label-hidden field__item")[0]
                temp_text_p_list = temp_text_div.find_all('p')

                temp_text_list = []
                for p in temp_text_p_list:
                    temp_text_list.append(p.text)

                temp_text = ' '.join(temp_text_list).replace("\xa0", "")
            except:
                temp_text = np.nan

            try:
                temp_text_div = soup.find_all('div', class_ = "clearfix text-formatted field field--name-field-text-column field--type-text-long field--label-hidden field__item")[0]
                temp_text_h3_list = temp_text_div.find_all('h3')

                temp_h3_list = []
                for h3 in temp_text_h3_list:
                    temp_h3_list.append(h3.text)

                h3_text = ' '.join(temp_h3_list).replace("\xa0", "")
            except:
                h3_text = np.nan
            main_text = temp_text +" "+ h3_text
            temp_dict = {
                'title': title,
                'date' : date,
                'category' : category,
                'summary' : summary,
                'main_text': main_text
            }
            temp_df = pd.DataFrame(temp_dict.values()).T
            temp_df.columns = temp_dict.keys()
            df = pd.concat([df, temp_df], ignore_index=True)
    except:
        print('Conection Error')
df.to_csv('../files/articles.csv', index=False)  