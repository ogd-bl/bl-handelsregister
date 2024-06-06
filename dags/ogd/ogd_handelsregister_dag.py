# DAG documentation
dag_doc = '''
This DAG is responsible for updating the cantonal inventory of companies, and the commerce registry messages for mutations and relocations.

- **update_inventory:** Synchronizes the company inventory on our db with the Zefix API and uploads the new data set to our ftp server
- **update_mutations_and_relocations**: Synchronizes the mutations and the relocations tables in our db with the Zefix API and uploads the new data sets to our ftp server
'''

### importing the required libraries
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator

from pendulum import duration
from datetime import datetime, timedelta

import os
import json
import requests

from dotenv import load_dotenv

# Load environment variables from env file
load_dotenv()

# Load config parameters from config file
config_file = 'ogd_handelsregister_config.json'
config_file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), config_file) # Make sure that the relatively placed file is found by constructing the absolute path

with open(config_file_path, 'r') as file: # Read config file
    config = json.load(file)


# Import utility functions for database and file management
from ogd.ogd_etl_utilities import *
from ogd.ogd_handelsregister_utilities import *

# Set up or update a PostgreSQL connection
create_or_update_postgres_conn('ogd_analytics_connection_id', 'postgres', os.getenv('ogd_analytics_host'), os.getenv('ogd_analytics_db'), os.getenv('ogd_analytics_user'), os.getenv('ogd_analytics_password'), os.getenv('ogd_analytics_port'))


update_inventory_doc = '''
This task first synchronizes the last two days on Zefix with our inventory table in our db. It then downloads the full table from our db
and uploads it to our ftp server.

A detailed description of how the inventory is synchronized can be found in the doc string of the method `update_sogc_messages`
'''
def update_inventory():
    # Set params
    directory = config['ODS_FTP_DIRECTORY']
    file_name = 'ods_handelsregister_inventory.csv' 
    host = os.getenv('ogd_ftp_host').replace('!', r'\!').replace('$', r'\\\$')
    user = os.getenv('ogd_ftp_user').replace('!', r'\!').replace('$', r'\\\$')
    pwd = os.getenv('ogd_ftp_password').replace('!', r'\!').replace('$', r'\\\$')

    # Synchronize data from the last week
    end_date = datetime.today()
    start_date = end_date - timedelta(days=7)

    # Update data in db
    with requests.Session() as session_zefix:
        session_zefix.proxies.update(get_default_proxies())
        session_zefix.auth = (os.getenv('ogd_zefix_user'), os.getenv('ogd_zefix_password'))
        update_companies_inventory(start_date, end_date, session_zefix)

    # Get table from db
    df = get_inventory_ogd_table()
    
    # Replace nan with empty strings for better display in ODS
    df.fillna('', inplace=True)

    # Handle all data as strings for better comparability upon next upload
    df = df.astype(str)

    # Test if all corrected addresses are still in use
    address_correction_string = address_corrections_digest(df)

    # Update file on ftp server and send digest mail
    update_with_digest(host, user, pwd, directory, file_name, df, address_correction_string, ['datum'])



update_mutations_and_relocations_doc = '''
This task first synchronizes the last two days on Zefix with our mutations table and the relocations table in our db. It then downloads the full
tables from our db and uploads them as files to our ftp server.

A detailed description of how the tables are synchronized can be found in the doc string of the method `update_sogc_messages`
'''
def update_mutations_and_relocations():
    # Set params
    directory = config['ODS_FTP_DIRECTORY']
    file_name_mut = 'ods_handelsregister_mutations.csv' 
    file_name_reloc = 'ods_handelsregister_relocations.csv' 
    host = os.getenv('ogd_ftp_host').replace('!', r'\!').replace('$', r'\\\$')
    user = os.getenv('ogd_ftp_user').replace('!', r'\!').replace('$', r'\\\$')
    pwd = os.getenv('ogd_ftp_password').replace('!', r'\!').replace('$', r'\\\$')
    end_date = datetime.today()

    # Synchronize data from the last week
    start_date = end_date - timedelta(days=7)

    # Update data in db
    with requests.Session() as session_zefix:
        session_zefix.proxies.update(get_default_proxies())
        session_zefix.auth = (os.getenv('ogd_zefix_user'), os.getenv('ogd_zefix_password'))
        update_sogc_messages(start_date, end_date, session_zefix)

    # Get table from db
    df_mut = get_mutations_ogd_table()
    df_reloc = get_relocations_ogd_table()
    
    # Replace nan with empty strings for better display in ODS
    df_mut.fillna('', inplace=True)
    df_reloc.fillna('', inplace=True)

    # Handle all data as strings for better comparability upon next upload
    df_mut = df_mut.astype(str)
    df_reloc = df_reloc.astype(str)

    # Update file on ftp server and send digest mail
    update_with_digest(host, user, pwd, directory, file_name_mut, df_mut)
    update_with_digest(host, user, pwd, directory, file_name_reloc, df_reloc)


# DAG definition
with DAG(
    dag_id='ogd_handelsregister',
    start_date=datetime(2024, 1, 1),
    schedule_interval='0 7 * * *',
    catchup=False,
    tags=['ogd'],
    access_control={'fb_ogd': {'can_read', 'can_edit'}},
    default_args = { # Default settings applied to all tasks
        'owner': 'ogd',
        'depends_on_past': False,
        'email': os.getenv('ogd_alert_mails').split(','),
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': duration(minutes=15)
    },
    doc_md=dag_doc
) as dag:

    update_inventorys_task = PythonOperator(
        task_id='update_inventory',
        python_callable=update_inventory,
        doc_md=update_inventory_doc
    )

    update_mutations_and_relocations_task = PythonOperator(
        task_id='update_mutations_and_relocations',
        python_callable=update_mutations_and_relocations,
        doc_md=update_mutations_and_relocations_doc
    )


    [update_mutations_and_relocations_task, update_inventorys_task]
    