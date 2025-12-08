FROM apache/airflow:2.9.1-python3.11

USER root
# Install Docker CLI so we can mount /var/run/docker.sock and use DockerOperator
RUN apt-get update && \
    apt-get install -y docker.io && \
    apt-get clean

# Give airflow user permission to use docker socket (in potential runtime scenarios, mostly group id alignment matters)
# We will handle group permissions via docker-compose (user: "${AIRFLOW_UID}:0")

USER airflow

# Install required providers
RUN pip install apache-airflow-providers-docker apache-airflow-providers-http apache-airflow-providers-postgres
