import datetime as dt

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import BranchPythonOperator

dag = DAG(
    dag_id='branch_dummy',
    schedule_interval='@once',
    start_date=dt.datetime(2023, 1, 30)
)



Task_1 = EmptyOperator(task_id='Task_1', dag=dag)


Task_2 = EmptyOperator(task_id='Task_2', dag=dag)
Task_3 = EmptyOperator(task_id='Task_3', dag=dag)

Task_4 = EmptyOperator(task_id='Task_4', dag=dag)
Task_5 = EmptyOperator(task_id='Task_5', dag=dag)
Task_6 = EmptyOperator(task_id='Task_6', dag=dag)

join = EmptyOperator(task_id='join', dag=dag)

Task_1 >> Task_2 >> join
Task_1 >> Task_3 >> join

join >> Task_4
join >> Task_5
join >> Task_6