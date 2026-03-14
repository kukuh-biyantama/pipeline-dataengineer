from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "dibimbing",
    "retry_delay": timedelta(minutes=5),
}

spark_dag = DAG(
    dag_id="spark_airflow_dag",
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    description="Test for spark submit",
    start_date=days_ago(1),
)

Extract = BashOperator(
    task_id="spark_submit_task",
    bash_command="""
        export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 && \
        spark-submit \
        --master local[2] \
        --conf spark.executor.memory=1g \
        --conf spark.driver.memory=1g \
        --conf spark.executor.cores=1 \
        /opt/airflow/spark-scripts/spark-example.py
    """,
    dag=spark_dag,
)

Extract
