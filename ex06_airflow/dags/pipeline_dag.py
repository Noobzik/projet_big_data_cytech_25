from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from docker.types import Mount
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    'project_pipeline',
    default_args=default_args,
    description='Automated pipeline for Big Data Project',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Tâche 1 : Création des tables SQL via DockerOperator
    create_tables = DockerOperator(
        task_id='create_tables',
        image='postgres:13',
        api_version='auto',
        auto_remove=True,
        command='psql -h postgres -U postgres -d warehouse -f /sql/creation.sql',
        docker_url='unix://var/run/docker.sock',
        network_mode='spark-network',
        environment={
            'PGPASSWORD': 'password'
        },
        mounts=[
            Mount(source='/Users/zigzeug/Documents/GitHub/projet_big_data_cytech_25/ex03_sql_table_creation', target='/sql', type='bind')
        ]
    )

    # Tâche 2 : Insertion des données de référence
    insert_data = DockerOperator(
        task_id='insert_reference_data',
        image='postgres:13',
        api_version='auto',
        auto_remove=True,
        command='psql -h postgres -U postgres -d warehouse -f /sql/insertion.sql',
        docker_url='unix://var/run/docker.sock',
        network_mode='spark-network',
        environment={
            'PGPASSWORD': 'password'
        },
        mounts=[
            Mount(source='/Users/zigzeug/Documents/GitHub/projet_big_data_cytech_25/ex03_sql_table_creation', target='/sql', type='bind')
        ]
    )

    # Tâche 3 : Job Spark de Validation
    spark_job = DockerOperator(
        task_id='spark_data_validation',
        image='bitnami/spark:3.5.3',
        api_version='auto',
        auto_remove=True,
        # Montage du volume pour accès au JAR compilé
        command='''spark-submit 
                      --class DataValidation 
                      --master spark://spark-master:7077 
                      --deploy-mode client 
                      /app/ex02_data_ingestion/target/scala-2.13/ex02_data_ingestion_2.13-0.1.jar''',
        docker_url='unix://var/run/docker.sock',
        network_mode='spark-network',
        environment={
            'DB_HOST': 'postgres',
            'DB_PORT': '5432',
            'MINIO_ENDPOINT': 'http://minio:9000'
        },
        mounts=[
            Mount(source='/Users/zigzeug/Documents/GitHub/projet_big_data_cytech_25', target='/app', type='bind')
        ]
    )

    # Tâche 4 : Déclenchement entrainement ML
    trigger_training = SimpleHttpOperator(
        task_id='trigger_ml_training',
        http_conn_id='ml_prediction_service',
        endpoint='/train',
        method='POST',
        headers={"Content-Type": "application/json"},
        log_response=True
    )

    create_tables >> insert_data >> spark_job >> trigger_training
