from __future__ import print_function
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime
import os
# from google_drive_downloader import GoogleDriveDownloader as gdd

import requests
import zipfile
import warnings
from sys import stdout
from os import makedirs
from os.path import dirname
from os.path import exists


class GoogleDriveDownloader:
    """
    Minimal class to download shared files from Google Drive.
    """

    CHUNK_SIZE = 32768
    DOWNLOAD_URL = 'https://docs.google.com/uc?export=download'

    @staticmethod
    def download_file_from_google_drive(file_id, dest_path, overwrite=False, unzip=False, showsize=False):
        """
        Downloads a shared file from google drive into a given folder.
        Optionally unzips it.

        Parameters
        ----------
        file_id: str
            the file identifier.
            You can obtain it from the sharable link.
        dest_path: str
            the destination where to save the downloaded file.
            Must be a path (for example: './downloaded_file.txt')
        overwrite: bool
            optional, if True forces re-download and overwrite.
        unzip: bool
            optional, if True unzips a file.
            If the file is not a zip file, ignores it.
        showsize: bool
            optional, if True print the current download size.
        Returns
        -------
        None
        """

        destination_directory = dirname(dest_path)
        if not exists(destination_directory):
            makedirs(destination_directory)

        if not exists(dest_path) or overwrite:

            session = requests.Session()

            print('Downloading {} into {}... '.format(
                file_id, dest_path), end='')
            stdout.flush()

            response = session.post(GoogleDriveDownloader.DOWNLOAD_URL, params={
                                    'id': file_id, 'confirm': 't'}, stream=True)

            if showsize:
                print()  # Skip to the next line

            current_download_size = [0]
            GoogleDriveDownloader._save_response_content(
                response, dest_path, showsize, current_download_size)
            print('Done.')

            if unzip:
                try:
                    print('Unzipping...', end='')
                    stdout.flush()
                    with zipfile.ZipFile(dest_path, 'r') as z:
                        z.extractall(destination_directory)
                    print('Done.')
                except zipfile.BadZipfile:
                    warnings.warn(
                        'Ignoring `unzip` since "{}" does not look like a valid zip file'.format(file_id))

    @staticmethod
    def _save_response_content(response, destination, showsize, current_size):
        with open(destination, 'wb') as f:
            for chunk in response.iter_content(GoogleDriveDownloader.CHUNK_SIZE):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
                    if showsize:
                        print(
                            '\r' + GoogleDriveDownloader.sizeof_fmt(current_size[0]), end=' ')
                        stdout.flush()
                        current_size[0] += GoogleDriveDownloader.CHUNK_SIZE

    # From https://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
    @staticmethod
    def sizeof_fmt(num, suffix='B'):
        for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
            if abs(num) < 1024.0:
                return '{:.1f} {}{}'.format(num, unit, suffix)
            num /= 1024.0
        return '{:.1f} {}{}'.format(num, 'Yi', suffix)


# Default arguments for the DAG
default_args = {
    'owner': 'your_name',
    'start_date': datetime(2023, 9, 25),
    'retries': 1,
}

# Create a DAG instance
dag = DAG('data_processing_dag',
          default_args=default_args,
          schedule_interval=None,
          catchup=False)

# Task 1: Dummy Operators for Start and End
start_task = DummyOperator(task_id='start', dag=dag)
end_task = DummyOperator(task_id='end', dag=dag)

# Task 2: Branching Task


def check_files_exist():
    # Check if Questions.csv and Answers.csv exist
    if os.path.isfile('/usr/local/share/data/Questions.csv') and os.path.isfile('/usr/local/share/data/Answers.csv'):
        return 'end_task'
    else:
        return 'clear_files'


branching_task = BranchPythonOperator(
    task_id='branching',
    provide_context=True,
    python_callable=check_files_exist,
    dag=dag
)

# Task 3: Clear Files
clear_files_task = BashOperator(
    task_id='clear_files',
    bash_command='rm -f /usr/local/share/data/Questions.csv /usr/local/share/data/Answers.csv',
    dag=dag
)

# Task 4: Download Question File


def download_question_file():
    GoogleDriveDownloader.download_file_from_google_drive(file_id='1TmM9LZl5eoW4Qvj4FS4fRWvuefF7uyjH',
                                                          dest_path='/usr/local/share/data/Questions.csv',
                                                          unzip=False)


download_question_file_task = PythonOperator(
    task_id='download_question_file_task',
    provide_context=True,
    python_callable=download_question_file,
    dag=dag
)

# Task 5: Download Answer File


def download_answer_file():
    GoogleDriveDownloader.download_file_from_google_drive(file_id='1CpqEm_xyQdsSPcuuoSf_2qmz4D6Q17En',
                                                          dest_path='/usr/local/share/data/Answers.csv',
                                                          unzip=False)


download_answer_file_task = PythonOperator(
    task_id='download_answer_file_task',
    provide_context=True,
    python_callable=download_answer_file,
    dag=dag
)

# Task 6: Import Questions into MongoDB
import_questions_mongo = BashOperator(
    task_id='import_questions_mongo',
    bash_command='mongoimport --type csv \
  --host mongo \
  --port 27017 \
  --username admin \
  --password password \
  --authenticationDatabase admin \
  --db stackoverflow \
  --collection questions \
  --headerline \
  --drop \
  --file /usr/local/share/data/Questions.csv',
    dag=dag,
)

# Task 7: Import Answers into MongoDB
import_answers_mongo = BashOperator(
    task_id='import_answers_mongo',
    bash_command='mongoimport --type csv \
  --host mongo \
  --port 27017 \
  --username admin \
  --password password \
  --authenticationDatabase admin \
  --db stackoverflow \
  --collection questions \
  --headerline \
  --drop \
  --file /usr/local/share/data/Answers.csv',
    dag=dag,
)

# Task 8: Spark Process
spark_process = SparkSubmitOperator(
    task_id='spark_process',
    conn_id='spark_default',
    application='/usr/local/share/spark/spark_script.py',
    packages='org.mongodb.spark:mongo-spark-connector_2.12:3.0.1',
    dag=dag
)

# Task 9: Import Output into MongoDB
import_output_mongo = BashOperator(
    task_id='import_output_mongo',
    bash_command='mongoimport --type csv -d stackoverflow -c results --headerline --drop /usr/local/share/data/output_file.csv',
    dag=dag
)

# Define the task dependencies
start_task >> branching_task
branching_task >> [clear_files_task, end_task]
clear_files_task >> [download_question_file_task, download_answer_file_task]
download_question_file_task >> import_questions_mongo
download_answer_file_task >> import_answers_mongo
[import_questions_mongo, import_answers_mongo] >> spark_process
spark_process >> import_output_mongo
import_output_mongo >> end_task
spark_process
