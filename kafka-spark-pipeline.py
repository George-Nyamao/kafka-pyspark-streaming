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
    task_id='start_zookeeper',
    bash_command='sh /home/morara/start-zoo.sh',
    dag=dag,
)
t2 = BashOperator(
    task_id='start_kafka_server',
    depends_on_past=False,
    bash_command='sh /home/morara/start-kafka.sh ',
    dag=dag,
)
t3 = BashOperator(
    task_id='start_hdfs',
    depends_on_past=False,
    bash_command='sh /home/morara/had.sh ',
    dag=dag,
)
t4 = BashOperator(
    task_id='start_producer',
    depends_on_past=False,
    bash_command='/usr/bin/python3 /home/morara/capstone-producer.py ',
    dag=dag,
)
t5 = BashOperator(
    task_id='start_producer',
    depends_on_past=False,
    bash_command='/usr/bin/python3 /home/morara/capstone-consumer.py ',
    dag=dag,
)
t6 = BashOperator(
    task_id='visualization',
    depends_on_past=False,
    bash_command='/usr/bin/python3 /home/morara/Documents/BigData/Kafka/visualization.py ',
    dag=dag,
)

(t1, t2, t3) >> t4 >> t5 >> t6

#t1.set_downstream(t2)
#t2.set_downstream(t3)
#t3.set_downstream(t4)
