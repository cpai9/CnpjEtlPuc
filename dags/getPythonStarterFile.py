from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task
from CNPJETL.src.moveProcessing import MoveArquivosCNPJ
from CNPJETL.src.convertCsvParquet import transformaCsvParquet
from CNPJETL.src.mountingDataFra import montarIngeriDataframes

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

@dag(dag_id="aIngestionCnpj",default_args=default_args, schedule="*/10 3-6 12-22 * *", catchup=False, tags=['CNPJ'])
def exec_ingestion():
    
    t0 = FileSensor(
        task_id='wait_for_file',
        fs_conn_id='connWatcherCNPJ',
        filepath='/home/uoperator/workdir/logsOperation/DirFlagSensor/StarterProcessCNPJ.txt',
        poke_interval=10,
        timeout=45
    )

    t1 = PythonOperator(
        task_id='move_csv',
        python_callable=MoveArquivosCNPJ,
    )

    t2 = PythonOperator(
        task_id='transforma_Parquet',
        python_callable=transformaCsvParquet,
    )

    t3 = PythonOperator(
        task_id='ingestao_dataframes',
        python_callable=montarIngeriDataframes,
    )

    t4 = BashOperator(
        task_id='limpeza_starter',
        bash_command="""
        rm -Rf /home/uoperator/workdir/logsOperation/DirFlagSensor/StarterProcessCNPJ.txt
        """)

    t0 >> t1 >> t2 >> t3 >> t4

execucao = exec_ingestion()
  