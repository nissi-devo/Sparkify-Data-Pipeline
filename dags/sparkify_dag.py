from datetime import datetime, timedelta
import pendulum
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.helpers import SqlQueries


default_args = {
    'owner': 'udacity',
    'start_date': datetime.now(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'depends_on_past': False,
    'catchup': False 
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    table="staging_events",
    s3_bucket="sparkify-dw",
    s3_key="log_data",
    json_path="s3://sparkify-dw/log_json_path.json",
    file_type="json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    table="staging_songs",
   s3_bucket="sparkify-dw",
    s3_key="song_data/A/A/A",
    json_path="auto",
    file_type="json"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='songplays',
    sql=SqlQueries.songplay_table_insert,
    load_method = 'append'
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql=SqlQueries.user_table_insert,
    table='users',
    load_method = 'overwrite'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql=SqlQueries.song_table_insert,
    table='songs',
    load_method = 'overwrite'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql=SqlQueries.artist_table_insert,
    table='artists',
    load_method = 'overwrite'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql=SqlQueries.time_table_insert,
    table='time',
    load_method = 'overwrite'
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    tables=['songplays', 'users', 'songs', 'artists', 'time'],
    dq_checks=[{'test_name':'completeness', 'expected_result': 1, 'comprison':'<'}]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> stage_songs_to_redshift
start_operator >> stage_events_to_redshift

stage_songs_to_redshift >> load_songplays_table
stage_events_to_redshift >> load_songplays_table

#Group tasks that branch out from load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]


#Group tasks that feed into run_quality_checks
[load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator