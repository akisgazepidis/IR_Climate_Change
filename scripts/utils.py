import psycopg2
import pandas as pd
import json
import numpy as np
import pandas as pd 
from time import sleep
import dateutil.parser
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
#from tqdm.notebook import tqdm as tqdm # Explain loops

def check_postgres_connection():
    try:
        conn = psycopg2.connect(
        host="localhost",
        port=5434,
        database="Climate_Change_DB",
        user="postgres",
        password="212121fg!"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        if cursor.fetchone()[0] == 1:
            print("Successful Test Connection to PostgreSQL")
        conn.close()
        return True
    except Exception as e:
        print("Failed test connection to PostgreSQL:", e)
        return False


def create_postgres_connection():
    print('Connecting to Postgres DB...')
    conn = psycopg2.connect(
        host="localhost",
        port=5434,
        database="Climate_Change_DB",
        user="postgres",
        password="212121fg!"
    )
    return conn

def create_postges_table_if_not_exist(conn,cursor):
    print('Checking if table exist..')
    create_table_query = """
    CREATE TABLE IF NOT EXISTS articles (
        id INTEGER PRIMARY KEY,
        link TEXT,
        title TEXT,
        date TIMESTAMP,
        category TEXT,
        main_text TEXT,
        headers TEXT
    );
    """

    cursor.execute(create_table_query)
    conn.commit()
    # cursor.close()
    # conn.close()

def create_scraped_url_list(max_page):
    url_list = []
    main_link = "https://news.un.org/"
    file_path = "files/url_list.txt"
    print('Url Scraping began...')

    for pg in range(max_page):
        url = "https://news.un.org/en/news/topic/climate-change?page="+ str(pg)
        #print(url)

        response = requests.get(url, timeout= 25)
        sleep(np.random.uniform(3, 4, 1)[0])
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
    print('Url Scraping finished')
    return url_list

def main_scrape(url_clean_list):
    print('Main info Scraping began...')
    main_list = []
    for url in tqdm(url_clean_list[:5]):
        #print(url)
        try:
            response = requests.get(url, timeout= 25)
            sleep(np.random.uniform(3, 4, 1)[0])
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

                    temp_text = ' \a '.join(temp_text_list).replace("\xa0", "")
                except:
                    temp_text = np.nan

                try:
                    temp_text_div = soup.find_all('div', class_ = "clearfix text-formatted field field--name-field-text-column field--type-text-long field--label-hidden field__item")[0]
                    temp_text_h3_list = temp_text_div.find_all('h3')

                    temp_h3_list = []
                    for h3 in temp_text_h3_list:
                        temp_h3_list.append(h3.text)

                    h3_text = ' \a '.join(temp_h3_list).replace("\xa0", "")
                except:
                    h3_text = np.nan
                link = url
                temp_dict = {
                    'link' : link,
                    'title': title,
                    'date' : date,
                    'category' : category,
                    'summary' : summary,
                    'main_text': temp_text,
                    'headers' : h3_text
                }
                main_list.append(temp_dict)
        except:
            print('Conection Error')
    print('Main info Scraping finished')
    return main_list

def check_article_exists(conn, link):
    print('Checking if article exist..')
    df = pd.read_sql_query("""
    SELECT count(*)
    FROM articles
    where link = %s 
    """, conn, params=[link])
    if df['count'][0] > 0:
        #print('Article exists')
        return True
    else:
        #print("Article doesn't exist")
        return False

def update_articles_table(conn, cursor, main_list):
    print('Updating articles table...')
    counter = 0
    for article in tqdm(main_list):
            id  = article["link"].split('/')[-1].split('-')[0]
            link = article["link"]
            title = article["title"]

            date_str = article['date']
            parsed_date = dateutil.parser.parse(date_str)
            date = parsed_date.strftime("%d/%m/%Y")

            category = article["category"]
            main_text = article["main_text"]
            headers = article["headers"]

            insert_query = f"INSERT INTO articles (id, link, title, date, category, main_text, headers) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            values = (id, link, title, date, category, main_text, headers)

            cursor.execute(insert_query, values)
            conn.commit()

            counter +=1
    print(f'{counter} records inserted.')

def clean_url_list(url_list, conn):
    print(f'Cleaning url list with {len(url_list)} elements...')
    temp_list = url_list.copy()

    for url in tqdm(temp_list):
        article_exists = check_article_exists(conn,str(url) )
        if article_exists:
            url_list.remove(url)
        else:
            pass
    print(f'List cleaned now {len(url_list)} exist...')
    return url_list

