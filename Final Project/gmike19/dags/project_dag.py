from datetime import datetime
from itertools import accumulate

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from airflow.operators.bash import BashOperator

args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1)
}

with DAG(
        dag_id='movies',
        default_args=args,
        schedule_interval='0 0 * * sun',
        tags=['added'],
        max_active_runs=1,
        catchup=False,
) as dag:
    transform_and_write_data = SparkSubmitOperator(
        application="/airflow/jobs/transform_data.py", task_id="transform_and_write",
    )

    write_genres = SparkSubmitOperator(
        application="/airflow/jobs/genres_to_csv.py", task_id="write_genres_to_csv",
        application_args=[Variable.get("num_posters_to_download")]
    )

    transform_and_write_data >> write_genres


    def get_index_values(num_posters, num_processes, **kwargs):
        nums = [num_posters // num_processes + int(x < num_posters % num_processes) for x in range(num_processes)]
        acc = list(accumulate(nums))
        res = [(x - nums[idx], x) for idx, x in enumerate(acc)]
        string_res = str(list(sum(res, ()))).replace(',', '').replace('[', '').replace(']', '')
        kwargs['ti'].xcom_push(key='indices', value=string_res)


    get_indices = PythonOperator(
        task_id='get_indices_for_download_processes',
        python_callable=get_index_values,
        op_kwargs={'num_posters': int(Variable.get("num_posters_to_download")),
                   'num_processes': int(Variable.get("num_download_processes"))}
    )

    write_genres >> get_indices

    train_model = BashOperator(
        task_id='train_model',
        bash_command=f'python /airflow/jobs/train_model.py {Variable.get("num_epochs")}'
    )

    for i in range(int(Variable.get("num_download_processes"))):
        download_posters = BashOperator(
            task_id='download_posters_' + str(i),
            bash_command=f"python /airflow/jobs/download_movie_posters.py {i} $indices",
            env={'indices': '{{ ti.xcom_pull(key="indices")}}'}
        )
        get_indices >> download_posters
        download_posters >> train_model

    predict_genre = BashOperator(
        task_id='predict_genre',
        bash_command=f'python /airflow/jobs/predict_genre.py {Variable.get("poster_to_predict_path")}'
    )

    train_model >> predict_genre
