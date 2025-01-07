"""
Airflow DAG for running a Scrapy spider to collect data from Poshmark.

This DAG is designed to run a Scrapy spider called PoshmarkUSSpider to scrape data from Poshmark.
The spider can be executed in different environments (prod or non-prod).

The DAG consists of a single task that uses the PythonOperator to run the Scrapy spider.

To run this DAG, set the appropriate environment variable 'environment' to 'prod' or leave it empty for non-prod.

Note: Make sure to customize the paths and settings according to your project structure.

Requirements:
- Airflow (Apache Airflow): https://airflow.apache.org/
- Scrapy: https://scrapy.org/

"""

import logging
import os
import subprocess

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from scripts.load_data import load_data

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 7),
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}


def run_scrapy_spider():
    """
    Function to execute the Scrapy spider based on the environment variable.

    If the environment is set to 'prod', it changes the working directory
    and uses a virtual environment for the Scrapy command.
    Additionally, it logs the current working directory before executing the spider.

    Args:
        None

    Returns:
        None
    """

    environment = Variable.get("environment", "")
    if environment == "prod":
        os.chdir("/home/muhammad-safdar/projects/hades_scraper/scrapers")
        scrapy_path = "/home/muhammad-safdar/projects/hades_scraper/scrapers"
        command = f"{scrapy_path} crawl PoshmarkUSSpider"
    else:
        os.chdir("scrapers")
        command = "scrapy crawl PoshmarkUSSpider"

    current_directory = os.getcwd()
    logging.info("Current Directory:", current_directory)

    subprocess.run(command, shell=True)


dag = DAG(
    "PoshmarkUSSpider",
    default_args=default_args,
    description="Run Scrapy spider with Airflow to get data from Poshmark",
    schedule_interval="5 0 * * *",
    tags=["poshmark.com", "na"],
)

run_scrapy_task = PythonOperator(
    task_id="Run_PoshmarkUSSpider",
    python_callable=run_scrapy_spider,
    dag=dag,
    priority_weight=1,
)

load_data_task = PythonOperator(
    task_id="PoshmarkUSSpider_load_data", 
    python_callable=load_data,
    op_kwargs={"file_name": "PoshmarkUSSpider"},
    execution_timeout=timedelta(hours=12),
    provide_context=True,
    dag=dag,
    priority_weight=1,
    trigger_rule="all_done",
)

run_scrapy_task >> load_data_task

