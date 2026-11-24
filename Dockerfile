FROM apache/airflow:3.4.0

# Instala as dependências do projeto sem deixar o pip trocar a versão do Airflow.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
