from datetime import timedelta
import pandas as pd
from google.cloud import storage
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators import python_operator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

default_args = {
    'owner': 'Felipe',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['felipe@ferro.dev'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
        'ingestao_raw_test_2',
        schedule_interval=timedelta(days=1),
        default_args=default_args,
        is_paused_upon_creation=True,
        description='Data ingestion dag to BQ'
) as dag:
    def ingestion():
        table_id = 'raw.dado_vendas'

        bucket_name = "teste-boticario-felipe"

        file_path = 'data/landing/'

        files = [f'{file_path}Base 2017.xlsx', f'{file_path}Base_2018.xlsx', f'{file_path}Base_2019.xlsx']

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(files[0])
        data_bytes = blob.download_as_bytes()
        excel_data = pd.read_excel(data_bytes)
        for i in range(1, len(files)):
            blob = bucket.blob(files[i])
            data_bytes = blob.download_as_bytes()
            excel_data = pd.concat([excel_data, pd.read_excel(data_bytes)], axis=0)

        raw_data = pd.DataFrame(excel_data,
                              columns=['ID_MARCA', 'MARCA', 'ID_LINHA', 'LINHA', 'DATA_VENDA', 'QTD_VENDA'])
        non_dupe_data = raw_data.drop_duplicates(keep='last', subset=['ID_MARCA', 'ID_LINHA', 'DATA_VENDA'])

        non_dupe_data.to_gbq(table_id, if_exists='replace')


    def apply_procedures(task, procedure):
        create_tables = BigQueryOperator(
            task_id=task,
            sql=procedure,
            use_legacy_sql=False,
            dag=dag,
            depends_on_past=False)

        return create_tables


    raw_data_ingestion = python_operator.PythonOperator(
        task_id='Raw_Data_Ingestion',
        python_callable=ingestion)

    basic_treatment = apply_procedures("Basic_Treatment", "CALL procedures.treat_tables_vendas()")

    raw_data_ingestion >> basic_treatment