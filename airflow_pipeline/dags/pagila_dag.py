from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

# deafult arguement
default_args = {
    'owner': 'pagila',
    'start_date':datetime.now() - timedelta(days=3),
    'end_date': datetime(2020, 7, 8),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'catchup': False
}

# dag creation
dag = DAG('pagila_data_dag',
        default_args=default_args,
        description='Load pagila data into redshift with airflow',
        schedule_interval='@hourly',
        )

# start operation
start_operator = DummyOperator(task_id='Begin_Execution', dag=dag)


# stage tables to redshift
stage_payment_to_redshift = StageToRedshiftOperator(
    task_id='Stage_payment',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_payment',
    s3_bucket='pagila-data',
    s3_key='payment',
    region='us-east-2',
    delimeter=',',
    extra_params="CSV IGNOREHEADER 1",  
)

stage_address_to_redshift = StageToRedshiftOperator(
    task_id='Stage_address',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_address',
    s3_bucket='pagila-data',
    s3_key='address',
    region='us-east-2',
    delimeter=',',
    extra_params="CSV IGNOREHEADER 1", 
)

stage_city_to_redshift = StageToRedshiftOperator(
    task_id='Stage_city',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_city',
    s3_bucket='pagila-data',
    s3_key='city',
    region='us-east-2',
    delimeter=',',
    extra_params="CSV IGNOREHEADER 1",
)

stage_country_to_redshift = StageToRedshiftOperator(
    task_id='Stage_country',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_country',
    s3_bucket='pagila-data',
    s3_key='country',
    region='us-east-2',
    delimeter=',',
    extra_params="CSV IGNOREHEADER 1",
)

stage_customer_to_redshift = StageToRedshiftOperator(
    task_id='Stage_customer',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_customer',
    s3_bucket='pagila-data',
    s3_key='customer',
    region='us-east-2',
    delimeter=',',
    extra_params="CSV IGNOREHEADER 1",
)

stage_film_to_redshift = StageToRedshiftOperator(
    task_id='Stage_film',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_film',
    s3_bucket='pagila-data',
    s3_key='film',
    region='us-east-2',
    extra_params="IGNOREHEADER 1 ACCEPTINVCHARS ESCAPE"
)

stage_inventory_to_redshift = StageToRedshiftOperator(
    task_id='Stage_inventory',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_inventory',
    s3_bucket='pagila-data',
    s3_key='inventory',
    region='us-east-2',
    delimeter=',',
    extra_params="CSV IGNOREHEADER 1",
)

stage_language_to_redshift = StageToRedshiftOperator(
    task_id='Stage_language',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_language',
    s3_bucket='pagila-data',
    s3_key='language',
    region='us-east-2',
    delimeter=',',
    extra_params="CSV IGNOREHEADER 1",
)

stage_rental_to_redshift = StageToRedshiftOperator(
    task_id='Stage_rental',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_rental',
    s3_bucket='pagila-data',
    s3_key='rental',
    region='us-east-2',
    delimeter=',',
    extra_params="CSV IGNOREHEADER 1",
)

stage_staff_to_redshift = StageToRedshiftOperator(
    task_id='Stage_staff',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_staff',
    s3_bucket='pagila-data',
    s3_key='staff',
    region='us-east-2',
    delimeter=',',
    extra_params="CSV IGNOREHEADER 1",
)

stage_store_to_redshift = StageToRedshiftOperator(
    task_id='Stage_store',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_store',
    s3_bucket='pagila-data',
    s3_key='store',
    region='us-east-2',
    delimeter=',',
    extra_params="CSV IGNOREHEADER 1",
)


# load tables from redshift to redshift
load_date_table = LoadDimensionOperator(
    task_id='load_date_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='dates',
    sql_query=SqlQueries.date_table_insert,
    #insert_after_delete=True
)

load_customer_table = LoadDimensionOperator(
    task_id='load_customer_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='customer',
    sql_query=SqlQueries.customer_table_insert,
    #insert_after_delete=True
)

load_movie_table = LoadDimensionOperator(
    task_id='load_movie_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='movie',
    sql_query=SqlQueries.movie_table_insert,
    #insert_after_delete=True
)

load_store_table = LoadDimensionOperator(
    task_id='load_store_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='store',
    sql_query=SqlQueries.store_table_insert,
    #insert_after_delete=True
)

load_sales_fact_table = LoadFactOperator(
    task_id='load_sales_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='sales_fact',
    sql_query=SqlQueries.sales_fact_insert,
)

# run quality checks
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    check_query="SELECT count(*) FROM users where last_name is null",
    check_method=lambda x: (len(x) == 1 and len(x[0]) == 1 and x[0][0] == 0)
)

# dag ends
end_operator = DummyOperator(task_id='Stop_Execution',  dag=dag)


# dependencies initialisation
# start and stage operations
start_operator >> stage_payment_to_redshift
start_operator >> stage_address_to_redshift
start_operator >> stage_city_to_redshift
start_operator >> stage_country_to_redshift
start_operator >> stage_customer_to_redshift
start_operator >> stage_film_to_redshift
start_operator >> stage_inventory_to_redshift
start_operator >>  stage_language_to_redshift
start_operator >> stage_rental_to_redshift
start_operator >> stage_staff_to_redshift
start_operator >> stage_store_to_redshift

# load tables
stage_store_to_redshift >> load_date_table
stage_store_to_redshift >> load_customer_table
stage_store_to_redshift >> load_movie_table
stage_store_to_redshift >> load_store_table
stage_store_to_redshift >> load_sales_fact_table

# quality checks
load_date_table >> run_quality_checks
load_customer_table >> run_quality_checks
load_movie_table >> run_quality_checks
load_store_table >> run_quality_checks
load_store_table >> run_quality_checks
load_sales_fact_table >> run_quality_checks

# end operations
run_quality_checks >> end_operator