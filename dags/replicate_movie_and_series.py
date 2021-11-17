from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from peterparker import replicate_movies, replicate_series

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email': ['spiderman@superhero.org']
}
with DAG(
        'replicate_movie_and_series',
        default_args=default_args,
        description='Replicate data from file to DWH',
        schedule_interval="0 */1 * * *",
        start_date=datetime(2021, 11, 18),
        catchup=False,
        tags=['replication', 'peterparker'],
) as dag:
    movies = PythonOperator(
        task_id='movies',
        python_callable=replicate_movies.run
    )

    series = PythonOperator(
        task_id='series',
        python_callable=replicate_series.run
    )

