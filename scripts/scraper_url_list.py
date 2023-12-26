import requests
from bs4 import BeautifulSoup
from tqdm.notebook import tqdm as tqdm # Explain loops

import json
import pandas as pd 
from time import sleep
from random2 import random

url_list = []
main_link = "https://news.un.org/"
file_path = "files/url_list.txt"
max_page = 200

for pg in range(max_page):
    url = "https://news.un.org/en/news/topic/climate-change?page="+ str(pg)
    print(url)

    response = requests.get(url, timeout= 25)
    sleep(random.uniform(3,4))
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        h2_list = list(soup.find_all('h2', class_ = 'node__title'))

        for h2 in h2_list:
            try:
                href = h2.find('a').get('href')
                url_list.append(main_link + href)
            except:
                print('href not found.')
    else:
        print("Response code not 200.")
with open(file_path, "a") as f:
    for item in url_list:
        f.write(str(item) + "\n")
