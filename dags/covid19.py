# Importa bibliotecas gerais da DAG

import sys
import os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
sys.path.append("desafio-data-engineer")
from airflow.models import DAG, Variable
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from src.tasks.carga_trusted import carga_trusted
from src.tasks.carga_refined import carga_refined


# Definindo a DAG e seus argumentos
default_args = {
    'owner': 'airflow'
    ,'start_date': days_ago(1)
    ,'depends_on_past': False
    ,'retries': 1
    ,'retry_delay': timedelta(minutes=5)
}

with DAG(
    default_args=default_args
    ,dag_id="covid19"
    ,description='ETL de arquivos CSV de Covid19 e join com PySpark'
    ,catchup=False
) as dag:

    # Define caminhos relativos das origens e destinos
    # Caminhos devem ser definidos através de variáveis de mesmo nome no airflow, referenciando localização no FS
    relative_path_input_raw = Variable.get('COVID19_PATH_RAW')
    print(f"Log source raw: {relative_path_input_raw}")
    relative_path_output_trusted = Variable.get('COVID19_PATH_TRUSTED')
    print(f"Log target trusted: {relative_path_output_trusted}")
    relative_path_input_refined = Variable.get('COVID19_PATH_TRUSTED')
    print(f"Log source refined: {relative_path_input_refined}")
    relative_path_output_refined = Variable.get('COVID19_PATH_REFINED')
    print(f"Log target refined: {relative_path_output_refined}")

    # Definindo as tarefas da DAG
    t_start = DummyOperator(task_id='t_start')
    t_carga_trusted = carga_trusted(filepath_input=relative_path_input_raw, filepath_output=relative_path_output_trusted)
    t_carga_refined = carga_refined(filepath_input=relative_path_input_refined, filepath_output=relative_path_output_refined)
    
    t_start >> t_carga_trusted >> t_carga_refined

