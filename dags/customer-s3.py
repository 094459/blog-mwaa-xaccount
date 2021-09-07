# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import io
import sys
import os
import logging
import random
from datetime import timedelta
import pandas as pd
import boto3
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago

DAG_ID = os.path.basename(__file__).replace('.py', '')
def save_file_to_s3():
    n = random.randint(100, 10000)
    filename = f'demo_{n}.csv'
    local_file_path = f'/tmp/{filename}'
    demo_data = pd.DataFrame({'num_legs': [2, 4, 8, 0],
                              'num_wings': [2, 0, 0, 0],
                              'num_specimen_seen': [10, 2, 1, 8]},
                             index=['falcon', 'dog', 'spider', 'fish'])
    demo_data.to_csv(local_file_path, index=False)

    bucket_name = Variable.get('TEST_BUCKET')
    folder_name = Variable.get('TEST_FOLDER')
    s3_key = f'{folder_name}/boto3_{filename}'
    logging.info(f'local_file_path: {local_file_path}, bucket_name: {bucket_name}, key: {s3_key}')

    logging.info('Uploading CSV to S3 with boto3')
    s3 = boto3.resource('s3')
    try:
        s3.meta.client.upload_file(local_file_path, bucket_name, s3_key, ExtraArgs={'ACL': 'bucket-owner-full-control'})
    except Exception as e:
        logging.info(e)
        pass
    logging.info('Done uploading CSV to S3 with boto3')
    logging.info('Uploading CSV to S3 with S3HOOK')
    s3_hook = S3Hook()
    s3_key = f'{folder_name}/s3hook_{filename}'
    try:
        s3_hook.load_file(local_file_path, bucket_name=bucket_name, key=s3_key, acl_policy='bucket-owner-full-control')
    except Exception as e:
        logging.info(e)
        pass
    logging.info('Done uploading CSV to S3 with S3HOOK')

    logging.info('Uploading CSV to S3 with pandas')
    demo_data.to_csv(f's3://{bucket_name}/{folder_name}/pandas_{filename}', index=False)
    logging.info('Done uploading CSV to S3 with pandas')


def run_dag():
    save_file_to_s3()

dag_default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id=DAG_ID,
    default_args=dag_default_args,
    description='description',
    schedule_interval="0 10 * * *",
    start_date=days_ago(1),
    tags=['test'],
) as dag:
    test_dag = PythonOperator(
        task_id="test_dag",
        python_callable=run_dag
    )
