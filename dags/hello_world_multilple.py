from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator


def print_hello():
    return "Hello world!"


dag = DAG(
    "hello_world",
    description="Simple tutorial DAG",
    schedule_interval="* * * * *",
    start_date=datetime(2017, 3, 20),
    catchup=False,
)

start_task = DummyOperator(task_id="start_dummy_task", retries=3, dag=dag)
end_task = DummyOperator(task_id="end_dummy_task", retries=3, dag=dag)


hello_task = PythonOperator(task_id="hello_task", python_callable=print_hello, dag=dag)

bash_task_1 = PythonOperator(
    task_id="bash_task_1",
    bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }}"',
    dag=dag,
)
bash_task_2 = PythonOperator(
    task_id="bash_task_2",
    bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }}"',
    dag=dag,
)
bash_task_3 = PythonOperator(
    task_id="bash_task_3",
    bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }}"',
    dag=dag,
)
bash_task_4 = PythonOperator(
    task_id="bash_task_4",
    bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }}"',
    dag=dag,
)
bash_task_5 = PythonOperator(
    task_id="bash_task_5",
    bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }}"',
    dag=dag,
)


(
    start_task
    >> [bash_task_1, bash_task_2, bash_task_3, bash_task_4, bash_task_5]
    >> hello_task
    >> end_task
)
