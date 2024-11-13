from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import random
import re
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
from airflow.models import Base
from airflow.utils.db import provide_session
from sqlalchemy import Column, String, Float, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Database setup
Base = declarative_base()
engine = create_engine('sqlite:///sentiment_scores.db')  # Example SQLite DB; update as needed
Session = sessionmaker(bind=engine)
Base.metadata.create_all(engine)

def clean_text(text):
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def get_article_links_yourstory(search_url, search, driver):
    driver.get(search_url)
    search_box_id = 'q'
    search_box = driver.find_element(By.ID, search_box_id)
    search_box.send_keys(search)
    search_box.send_keys(Keys.RETURN)
    time.sleep(1)
    links = driver.find_elements(By.XPATH, '//a[@href and @data-sectiontype="default"]')
    article_links_search = []
    links_temp = []
    for link in links:
        article_url = link.get_attribute('href')
        if len(links_temp) > 4:
            break
        if article_url not in links_temp:
            links_temp.append(article_url)
            article_links_search.append({'ticker': search, 'article_url': article_url})
    return article_links_search

def get_articles_yourstory(article_links, driver):
    articles = []
    for article_link in article_links:
        driver.get(article_link['article_url'])
        title = driver.find_element(By.TAG_NAME, 'h1').text
        content_body = driver.find_elements(By.CLASS_NAME, 'quill-content')
        content = []
        for body in content_body:
            paragraphs = body.find_elements(By.TAG_NAME, 'p')
            content.append(' '.join([para.text for para in paragraphs]))
        final_content = ' '.join(content)
        final_content = clean_text(final_content)
        articles.append({'ticker': article_link['ticker'], 'title': title, 'content': final_content})
    return articles

def get_article_links_finshots(search_url, search, driver):
    driver.get(search_url)
    try:
        element = driver.find_element(By.CLASS_NAME, 'c-nav__item')
        element.click()
    except NoSuchElementException:
        toggle_element = driver.find_element(By.CLASS_NAME, 'site-mast-right-mobile')
        toggle_element.click()
        element = driver.find_element(By.CLASS_NAME, 'toggle-search-button js-search-toggle')
        element.click()
    search_box = driver.find_element(By.CLASS_NAME, 'c-search__input')
    search_box.send_keys(search)
    search_box.send_keys(Keys.RETURN)
    time.sleep(2)
    links = driver.find_elements(By.CLASS_NAME, 'c-search-result')
    links_temp = []
    article_links_search = []
    for link in links:
        article_url = link.get_attribute('href')
        if len(links_temp) > 4:
            break
        if article_url not in links_temp:
            links_temp.append(article_url)
            article_links_search.append({'ticker': search, 'article_url': article_url})
    return article_links_search

def get_articles_finshots(article_links, driver):
    articles = []
    for article_link in article_links:
        driver.get(article_link['article_url'])
        title = driver.find_element(By.CLASS_NAME, 'post-full-title').text
        content_body = driver.find_elements(By.CLASS_NAME, 'post-content')
        content = []
        for body in content_body:
            paragraphs = body.find_elements(By.TAG_NAME, 'p')
            content.append(' '.join([para.text for para in paragraphs]))
        final_content = ' '.join(content)
        final_content = clean_text(final_content)
        articles.append({'ticker': article_link['ticker'], 'title': title, 'content': final_content})
    return articles

def fetch_and_process_articles(**kwargs):
    options = Options()
    options.add_argument('--start-fullscreen')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    # options.add_argument('--headless')
    service = Service('/Users/Akash.A.Singh/Downloads/chromedriver-mac-arm64/chromedriver')
    driver = webdriver.Chrome(service=service, options=options)

    yourstory_url = 'https://yourstory.com/search'
    finshots_url = 'https://finshots.in/'
    article_links = []
    articles = []
    for search in ['HDFC', 'Tata Motors']:
        article_links_search = get_article_links_yourstory(yourstory_url, search, driver)
        article_links.extend(article_links_search)
    articles.extend(get_articles_yourstory(article_links, driver))
    article_links = []
    for search in ['HDFC', 'Tata Motors']:
        article_links_search = get_article_links_finshots(finshots_url, search, driver)
        article_links.extend(article_links_search)
    articles.extend(get_articles_finshots(article_links, driver))

    driver.quit()
    return articles

def generate_sentiment_score(article_text):
    return random.uniform(0, 1)

def generate_sentiment_score_task(**kwargs):
    articles = kwargs['ti'].xcom_pull(task_ids='fetch_and_process_articles')
    if articles is None:
        raise ValueError("No articles retrieved.")
    sentiment_scores = {article['title']: generate_sentiment_score(article['content']) for article in articles}
    kwargs['ti'].xcom_push(key='sentiment_scores', value=sentiment_scores)

class SentimentScore(Base):
    __tablename__ = 'sentiment_scores'
    title = Column(String, primary_key=True)
    score = Column(Float)

@provide_session
def persist_data(sentiment_scores, session=None):
    if session is None:
        session = Session()
    for title, score in sentiment_scores.items():
        sentiment_score = SentimentScore(title=title, score=score)
        session.merge(sentiment_score)
    session.commit()
    session.close()

def persist_data_task(**kwargs):
    sentiment_scores = kwargs['ti'].xcom_pull(key='sentiment_scores', task_ids='generate_sentiment_score')
    persist_data(sentiment_scores)

def send_alert(context):
    task_instance = context['task_instance']
    message = f"Task {task_instance.task_id} in DAG {task_instance.dag_id} failed."
    print(f"Sending alert: {message}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': send_alert,
}

with DAG('sentiment_analysis_pipeline_f', default_args=default_args, catchup=False) as dag:
    fetch_and_process = PythonOperator(
        task_id='fetch_and_process_articles',
        python_callable=fetch_and_process_articles,
        provide_context=True
    )
    generate_sentiment = PythonOperator(
        task_id='generate_sentiment_score',
        python_callable=generate_sentiment_score_task,
        provide_context=True
    )
    persist = PythonOperator(
        task_id='persist_data',
        python_callable=persist_data_task,
        provide_context=True
    )

    fetch_and_process >> generate_sentiment >> persist
