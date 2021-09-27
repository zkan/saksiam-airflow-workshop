import logging

from airflow import DAG
from airflow.utils import timezone
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator


def _say_hello():
    logging.info("Hello, SAKSIAM!")


default_args = {
    "owner": "Kan Ouivirach",
    "start_date": timezone.datetime(2021, 9, 27)
}
with DAG(
    "my_dag",
    schedule_interval="*/10 * * * *",
    default_args=default_args,
    catchup=False,
    tags=["saksiam"],
) as dag:

    start = DummyOperator(task_id="start")

    echo_hello = BashOperator(
        task_id="echo_hello",
        bash_command="echo 'hello'",
    )

    say_hello = PythonOperator(
        task_id="say_hello",
        python_callable=_say_hello,
    )

    end = DummyOperator(task_id="end")

    # start >> echo_hello >> say_hello >> end

    # start >> echo_hello
    # start >> say_hello
    # echo_hello >> end
    # say_hello >> end

    start >> [echo_hello, say_hello] >> end