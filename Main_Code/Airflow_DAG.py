import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import datetime

SNOWFLAKE_CONN_ID = 'snowflake_conn'

default_args = {
    "owner": "snowflakedatapipelinep50",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "p50_Final",
    default_args=default_args,
    description="Runs data pipeline",
    schedule_interval=None,
    is_paused_upon_creation=False,
)

bash_task = BashOperator(task_id="run_bash_echo", bash_command="echo 1", dag=dag)

post_task = BashOperator(task_id="post_dbt", bash_command="echo 0", dag=dag)

batch_id =str(datetime.datetime.now().strftime("%Y%m%d%H%M"))
print("BATCH_ID = " + batch_id)


 
task_customers_processing_to_processed = BashOperator(
 task_id="p50_processing_to_processed",
 bash_command='aws s3 mv s3://p50-bigdata-dns-or-unknown-attack-records/unsw-nb15/dns_or_unknown_attack_records/{0}/ s3://p50-bigdata-dns-or-unknown-attack-records/unsw-nb15/attack_records_processed/{0}/ --recursive'.format(batch_id),
 dag=dag
)


snowflake_query_P50_Final_Table = [
    """COPY INTO P50_FINAL_DB.P50_FINAL_SCHEMA.P50_FINAL_TABLE
  FROM @P50_DATA_STAGE;""".format(batch_id),
]


snowflake_customers_sql_str = SnowflakeOperator(
    task_id='snowflake_raw_insert_customers',
    dag=dag,
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=snowflake_query_P50_Final_Table,
    warehouse="P50_FINAL_WAREHOUSE",
    database="P50_FINAL_DB",
    schema="P50_FINAL_SCHEMA",
    role="ACCOUNTADMIN",
)



[ snowflake_customers_sql_str >> task_customers_processing_to_processed] >> post_task