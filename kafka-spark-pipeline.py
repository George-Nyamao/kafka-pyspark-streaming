from datetime import timedelta
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    # 'end_date': datetime(2020, 10, 22),
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    }
    
dag = DAG(
    'kafka-spark',
    default_args=default_args,
    description='A Kafka Spark Pipeline DAG',
    # Continue to run DAG once per day
    schedule_interval=timedelta(days=1),)
    
t1 = BashOperator(
    task_id='drop_hive_mysql_tables',
    depends_on_past=False,
    bash_command='sh /home/morara/drophivemysqltables.sh ',
    dag=dag,
)
t2 = BashOperator(
    task_id='clear_topic',
    depends_on_past=False,
    bash_command='sh /home/morara/empty_topic.sh ',
    dag=dag,
)
t3 = BashOperator(
    task_id='delay_five_sec',
    depends_on_past=False,
    bash_command='sleep 5 ',
    dag=dag,
)
t4 = BashOperator(
    task_id='start_consumer',
    depends_on_past=False,
    bash_command='sh /home/morara/cons_term.sh ',
    dag=dag,
)

t5 = BashOperator(
    task_id='start_producer',
    depends_on_past=False,
    bash_command='sh /home/morara/prod_term.sh ',
    dag=dag,
)
t6 = BashOperator(
    task_id='visualization',
    depends_on_past=False,
    bash_command='python3 /home/morara/Documents/BigData/Kafka/visualization.py ',
    dag=dag,
)

[t1, t2] >> t3 >> t4 >> t5 >> t6

#t1.set_downstream(t2)
#t2.set_downstream(t3)
#t3.set_downstream(t4)
