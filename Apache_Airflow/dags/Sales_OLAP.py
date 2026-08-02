from airflow import DAG # for initial workflow
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor  # for check for new files
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator #for sql statements excution
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook # for snowflake connection
from airflow.operators.empty import EmptyOperator # for start and end task 
from airflow.operators.bash import BashOperator # for cmd statement like dbt run and other excution
from airflow.operators.python import PythonOperator # for python logic excution
from airflow.operators.python import ShortCircuitOperator # for ensure of data existing
from airflow.providers.smtp.operators.smtp import EmailOperator# for sending email notification
from datetime import datetime, timedelta
import pendulum

local_tz = pendulum.timezone("Africa/Cairo")


default_args = {
    "owner": "adam",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

#check in aws s3 bucket
datasets = [
    "customers",
    "orders",
    "order_items",
    "products",
    "categories",
    "brands",
    "staffs",
    "stores",
    "stocks",
]
# check in snowflake
tables = [
    "CUSTOMERS",
    "ORDERS",
    "ORDER_ITEMS",
    "PRODUCTS",
    "CATEGORIES",
    "BRANDS",
    "STAFFS",
    "STORES",
    "STOCKS",
]


with DAG(
    dag_id="Sales_OLAP",
    default_args=default_args,
    description="End-to-end Sales OLAP pipeline using Snowflake and dbt automated with Airflow",
    start_date=pendulum.datetime(2026, 8, 2, 11, 20, tz=local_tz),
    schedule="20 11 * * *",
    catchup=False,
    tags=["sales", "OLAP", "snowflake", "dbt"],
    template_searchpath=["/usr/local/airflow/include"],  
) as dag:

    
    start_operator = EmptyOperator(
        task_id="START"
    )


    s3_sensors = []

    for dataset_table in datasets:
        sensor = S3KeySensor(
            task_id=f"wait_for_{dataset_table}",
            bucket_name="sales-database-files",
            bucket_key=f"{dataset_table}/*.csv",
            aws_conn_id="aws_default",
            wildcard_match=True, #to work with /*.csv  if wildcard_match is true it consider 
                                #any file in folders ends with .csv like customers_01_2020.csv is valid
            poke_interval=60, # كل قد إيه الـ Sensor يروح يسأل S3.  (60 means 1 minute)
            timeout=60 * 60,
            mode="reschedule", # mode="reschedule",  # Unlike "poke", it doesn't keep the worker busy while waiting
        )

        s3_sensors.append(sensor)


    load_bronze = SQLExecuteQueryOperator(
    task_id="load_bronze",
    conn_id="snowflake_default",
    sql="snowflake/load_in_bronze.sql",
    )


    def check_bronze():
        hook = SnowflakeHook(
            snowflake_conn_id="snowflake_default"
        )

        for table in tables:
            sql = f"""
            SELECT COUNT(*)
            FROM BRONZE.{table};
            """

            result = hook.get_first(sql)

            # لو الجدول فاضي
            if result is None or result[0] == 0:
                print(f"Table {table} is empty.")
                return False

        print("All Bronze tables contain data.")
        return True


    check_bronze_data = ShortCircuitOperator(
        task_id="check_bronze_data",
        python_callable=check_bronze,
    )

    run_dbt_silver = BashOperator(
    task_id="run_dbt_silver",
   bash_command="""
    cd /usr/local/airflow/dbt/OLAP_Sales_System
    dbt run --profiles-dir . --select silver
    """
    )

    run_dbt_gold = BashOperator(
    task_id="run_dbt_gold",
    bash_command="""
    cd /usr/local/airflow/dbt/OLAP_Sales_System && \
    dbt run --profiles-dir . --select gold
    """
    )


    run_dbt_tests = BashOperator(
    task_id="run_dbt_tests",
    bash_command="""
    cd /usr/local/airflow/dbt/OLAP_Sales_System && \
    dbt test --profiles-dir .    
    """
    )

    send_email_notification = EmailOperator(
        task_id="send_email_notification",
        to="adammohamedshaker3@gmail.com",
        subject="Airflow DAG Completed",
        html_content="""
        <h2>Sales OLAP Pipeline Completed Successfully ✅</h2>
        <p>Your Airflow DAG has finished successfully.</p>
        """,
    )

    end_operator = EmptyOperator(
        task_id='END'
    )

                            
    start_operator >>s3_sensors >> load_bronze >> check_bronze_data >> run_dbt_silver >> run_dbt_gold >> run_dbt_tests >> send_email_notification >> end_operator