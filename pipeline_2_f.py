from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime, timedelta
import os
import requests
import zipfile
import pandas as pd
from scipy.spatial.distance import cosine
import logging


def get_dataset(url, zip_path, extract_folder, **kwargs):
    if not os.path.exists(extract_folder):
        os.makedirs(extract_folder)

    response = requests.get(url, verify=False)
    with open(zip_path, 'wb') as file:
        file.write(response.content)
    logging.info(f"Saved zip file {zip_path}")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_folder)
    logging.info(f"extracted zip file {extract_folder}")
    os.remove(zip_path)

def load_dataframes(**kwargs):
    folder_path = kwargs['folder_path']
    col_names_user = ['user_id','Age', 'Gender', 'Occupation', 'Zip Code']
    df_users = pd.read_csv('http://files.grouplens.org/datasets/movielens/ml-100k/u.user', sep='|', header=None, names=col_names_user)
    col_names_data = ['user id', 'item id', 'rating', 'timestamp']
    df_data = pd.read_csv(f'http://files.grouplens.org/datasets/movielens/ml-100k/u.data', sep='\t', header=None, names=col_names_data)
    col_names = ['movie id', 'movie title', 'release date', 'video release date', 'IMDb URL', 'unknown', 
                    'Action', 'Adventure', 'Animation', 'Childrens', 'Comedy','Crime', 'Documentary', 'Drama',
                    'Fantasy', 'Film-Noir', 'Horror', 'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western']
    df_movies = pd.read_csv(f'http://files.grouplens.org/datasets/movielens/ml-100k/u.item', sep='|', encoding='latin-1', header=None, names=col_names, engine='python', on_bad_lines='skip', encoding_errors='ignore')

    ti = kwargs['ti']
    ti.xcom_push(key='df_users', value=df_users.to_json())
    ti.xcom_push(key='df_data', value=df_data.to_json())
    ti.xcom_push(key='df_movies', value=df_movies.to_json())


def get_mean_age(**kwargs):
    ti = kwargs['ki']
    df_users = pd.read_json(ti.xcom_pull(key='df_users', task_ids='load_dataframes'))
    mean_age_by_occupation = df_users.groupby('Occupation')['Age'].mean()
    
    mean_age_by_occupation.to_csv(f'{save_path}mean_age_by_occupation.csv')
    print(mean_age_by_occupation)

def top_rated_movies(**kwargs):
    ti = kwargs['ti']
    df_data = pd.read_json(ti.xcom_pull(key='df_data', task_ids='load_dataframes'))
    df_movies = pd.read_json(ti.xcom_pull(key='df_movies', task_ids='load_dataframes'))
    grouped_df = df_data.groupby('item id').filter(lambda x: x['user id'].nunique() > 35)
    average_ratings = grouped_df.groupby('item id')['rating'].mean()
    top_20_items = average_ratings.sort_values(ascending=False).head(20).reset_index()
    top_20_movies = pd.merge(top_20_items, df_movies, left_on='item id', right_on='movie id')

    print(top_20_movies[['movie title', 'rating']])
    top_20_movies.to_csv(f'{save_path}top_20_movies.csv')

def top_genres(**kwargs):
    ti = kwargs['ti']
    df_data = pd.read_json(ti.xcom_pull(key='df_data', task_ids='load_dataframes'))
    df_movies = pd.read_json(ti.xcom_pull(key='df_movies', task_ids='load_dataframes'))
    df_users = pd.read_json(ti.xcom_pull(key='df_users', task_ids='load_dataframes'))
    merged_df = pd.merge(df_data, df_users, left_on='user id', right_on='user_id')
    bins = [20, 25, 35, 45, float('inf')]
    labels = ['20-25', '25-35', '35-45', '45 and older']
    merged_df['age_group'] = pd.cut(merged_df['Age'], bins=bins, labels=labels, right=False)

    merged_df = pd.merge(merged_df, df_movies, left_on='item id', right_on='movie id')
    genre_columns = ['unknown', 'Action', 'Adventure', 'Animation', 'Childrens', 'Comedy','Crime', 'Documentary', 'Drama',
                'Fantasy', 'Film-Noir', 'Horror', 'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western']
    
    melted_df = merged_df.melt(id_vars=['user id', 'item id', 'rating', 'timestamp', 
                                    'Age', 'Gender', 'Occupation', 'Zip Code',
                                    'age_group'], value_vars=genre_columns,
                           var_name='genre', value_name='is_genre')
    melted_df = melted_df[melted_df['is_genre'] == 1]

    grouped_genres = melted_df.groupby(['age_group', 'Occupation', 'genre']).size().reset_index(name='count')
    top_genres = grouped_genres.loc[grouped_genres.groupby(['age_group', 'Occupation'])['count'].idxmax()]
    top_genres.to_csv(f'{save_path}top_genres.csv')
    print(top_genres)

def top_similar_movies(movie_title, top_n=10, **kwargs):
    ti = kwargs['ti']
    df_data = pd.read_json(ti.xcom_pull(key='df_data', task_ids='load_dataframes'))
    df_movies = pd.read_json(ti.xcom_pull(key='df_movies', task_ids='load_dataframes'))
    df_users = pd.read_json(ti.xcom_pull(key='df_users', task_ids='load_dataframes'))
    merged_df = pd.merge(df_data, df_users, left_on='user id', right_on='user_id')
    merged_df = pd.merge(merged_df, df_movies, left_on='item id', right_on='movie id')
    user_movie_matrix = merged_df.pivot_table(index='user_id', columns='movie title', values='rating').fillna(0)
    
    # Calculate the Pearson correlation coefficient
    movie_similarity = user_movie_matrix.corr(method='pearson')
    similar_movies = movie_similarity[movie_title].sort_values(ascending=False)[1:top_n+1]
    
    similar_movies.to_csv(f'{save_path}similar_movies_{movie_title}.csv')
    print(similar_movies)

# Define paths and URLs
directory_path = '/Users/Akash.A.Singh/Documents/Airflow/'
folder_path = f'{directory_path}ml-100k/'
url = "https://files.grouplens.org/datasets/movielens/ml-100k.zip"
save_path = f'{directory_path}Output Files/'
zip_path = f"{directory_path}ml-100k.zip"
if not os.path.exists(save_path):
    os.makedirs(save_path)

def send_alert(context):
    task_instance = context['task_instance']
    message = f"Task {task_instance.task_id} in DAG {task_instance.dag_id} failed."
    print(f"Sending alert: {message}")

# Define the default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    'on_failure_callback': send_alert,
}

# Define the DAG
with DAG(
        "movie_analysis_pipeline",
        default_args=default_args,
        description="A pipeline to analyze movie data",
        schedule_interval='0 20 * * 1-5',
        start_date=datetime(2023, 1, 1),
        catchup=False,
) as dag:
    
    wait_for_pipeline_1 = ExternalTaskSensor(
    task_id='wait_for_pipeline_1',
    external_dag_id='sentiment_analysis_pipeline_f',
    external_task_id='persist_data',
    mode='poke',
    timeout=600,
    poke_interval=60,
    dag=dag,
)

    task_get_dataset = PythonOperator(
        task_id="get_dataset",
        python_callable=get_dataset,
        op_kwargs={"url": url, "zip_path":zip_path, "extract_folder":folder_path},
        dag=dag
    )

    task_load_dataframes = PythonOperator(
        task_id="load_dataframes",
        python_callable=load_dataframes,
        op_kwargs={"folder_path":folder_path},
        dag=dag
    )

    task_get_mean_age = PythonOperator(
        task_id="get_mean_age",
        python_callable=get_mean_age,
        dag=dag,
    )

    task_top_rated_movies = PythonOperator(
        task_id="top_rated_movies",
        python_callable=top_rated_movies,
        dag=dag,
    )

    task_top_genres = PythonOperator(
        task_id="top_genres",
        python_callable=top_genres,
        dag=dag,
    )

    task_top_similar_movies = PythonOperator(
        task_id="top_similar_movies",
        python_callable=top_similar_movies,
        op_kwargs={"movie_title": "Toy Story (1995)"},
        dag=dag,
    )

wait_for_pipeline_1 >> task_get_dataset >> task_load_dataframes >> [task_get_mean_age, task_top_rated_movies, task_top_genres, task_top_similar_movies]