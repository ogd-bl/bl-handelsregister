import os

from airflow import settings
from airflow.models import Connection
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.email import send_email

import subprocess
import tempfile
import pandas as pd
import requests
import psycopg2


from dotenv import load_dotenv
load_dotenv()

###############################################
#### Connection and data handling helpers #####
###############################################

def create_or_update_postgres_conn(conn_id, conn_type, host, schema, login, password, port):
    '''
    Creates or updates a PostgreSQL connection in Airflow's metadata database.

    This function first attempts to retrieve an existing connection using the provided conn_id. If the connection does not exist, it creates a new one with the specified parameters. If the connection already exists, it updates the existing connection with the provided parameters.

    Parameters:
    - conn_id (str): The ID of the connection to create or update.
    - conn_type (str): The type of the connection (e.g., 'postgres').
    - host (str): The hostname of the PostgreSQL server.
    - schema (str): The schema name (database name) to connect to.
    - login (str): The username for authentication.
    - password (str): The password for authentication.
    - port (int): The port number on which the PostgreSQL server is running.

    Returns:
    None
    '''
    # Get the current session
    session = settings.Session()
    conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()

    # Create a new connection if it does not exist
    if conn is None:
        new_conn = Connection(
            conn_id=conn_id,
            conn_type=conn_type,
            host=host,
            schema=schema,
            login=login,
            password=password,
            port=port
        )
        session.add(new_conn)
        session.commit()
        print(f"Created new connection with ID: {conn_id}")

    # If connection exists, update the existing connection
    else:
        conn.conn_type = conn_type
        conn.host = host
        conn.schema = schema
        conn.login = login
        conn.password = password
        conn.port = port
        session.commit()

    # Close the session
    session.close()

def construct_absolute_file_path(relative_path, filename):
    '''
    Constructs an absolute file path from a relative path and filename.

    This function constructs an absolute path by combining the directory of the current Python file (__file__), a relative path, and a filename. It is useful for constructing file paths in scripts that need to reference other files within the same project structure.

    Parameters:
    - relative_path (str): The relative path from the current Python file to the target directory.
    - filename (str): The name of the target file.

    Returns:
    str: The absolute path to the file.
    '''

    # get dag path
    dag_directory = os.path.dirname(os.path.realpath(__file__))

    # construct absolute path
    file_path = os.path.join(dag_directory, relative_path, filename)

    return file_path

def extract_data_with_sql_script(script_path, postgres_conn_id):
    '''
    Extracts data from a PostgreSQL database by executing a SQL script.

    This function creates a PostgreSQL hook using the provided connection ID, reads a SQL script from the specified file path, and executes the script to fetch data. The data is then returned as a pandas DataFrame.

    Parameters:
    - script_path (str): The path to the SQL script file to be executed.
    - postgres_conn_id (str): The connection ID for the PostgreSQL database.

    Returns:
    pandas.DataFrame: A DataFrame containing the data fetched by executing the SQL script.
    '''

    # Create Postgres Hook
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    # Read the content of the SQL file
    with open(script_path, 'r') as file:
        sql_query = file.read()

    # Download data
    df = hook.get_pandas_df(sql=sql_query)

    return df

def extract_data_with_sql(sql_query, postgres_conn_id):
    '''
    Extracts data from a PostgreSQL database by executing an sql query.

    This function creates a PostgreSQL hook using the provided connection ID and executes the query to fetch data. The data is then returned as a pandas DataFrame.

    Parameters:
    - sql_query (str): An sql query
    - postgres_conn_id (str): The connection ID for the PostgreSQL database.

    Returns:
    pandas.DataFrame: A DataFrame containing the data fetched by executing the SQL script.
    '''

    # Create Postgres Hook
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    # Download data
    df = hook.get_pandas_df(sql=sql_query)

    return df


####################################################
########### File up- and download helpers ##########
####################################################

def save_pandas_to_csv_on_ftp(df, host, user, pwd, directory, file_name, sep=','):
    '''
    Saves a pandas DataFrame to a CSV file on an FTP server.

    This function takes a DataFrame and saves it as a CSV file on an FTP server. The connection details for the FTP server are retrieved from environment variables. It uses curl for the file transfer, which supports FTPS and proxy settings.

    Parameters:
    - df (pandas.DataFrame): The DataFrame to save.
    - directory (str): The directory on the FTP server where the file should be saved.
    - file_name (str): The name of the file to be saved on the FTP server.
    - sep (str, optional): The separator to use in the CSV file. Defaults to ','.

    Returns:
    None
    '''

    # Create a temporary file to write the DataFrame to CSV
    with tempfile.NamedTemporaryFile(mode='w+', delete=True) as tmpfile:
        
        csv_file_path = tmpfile.name

        # Save the DataFrame to the temporary CSV file
        df.to_csv(csv_file_path, index=False, sep=sep)
        
        # Prepare FTP connection details, escaping special characters
        https_proxy = os.getenv('https_proxy').replace('!', r'\!').replace('$', r'\\\$')

        # Construct and execute the curl command to upload the file
        curl_command = f'''curl -v -x {https_proxy} --ssl -u {user}:{pwd} -T {csv_file_path} ftps://{host}/{directory}/{file_name}'''
        process = subprocess.run(curl_command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

def save_file_to_ftp(host, user, pwd, directory, file, file_name):
    '''
    Saves a binary file to an FTP server.

    This function takes a binary file, writes it to a temporary file, and then uploads it to an FTP server using curl. The FTP server connection details are specified via environment variables.

    Parameters:
    - file (bytes): The binary content of the file to save.
    - directory (str): The directory on the FTP server where the file should be saved.
    - file_name (str): The name of the file to be saved on the FTP server.

    Returns:
    None
    '''

    # Write the binary file to a temporary file
    with tempfile.NamedTemporaryFile(mode='w+b', delete=True) as tmpfile:
        file_path = tmpfile.name
        tmpfile.write(file)

        # Prepare FTP connection details, escaping special characters
        https_proxy = os.getenv('https_proxy').replace('!', r'\!').replace('$', r'\\\$')

        # Construct and execute the curl command to upload the file
        curl_command = f'''curl -v -x {https_proxy} --ssl -u {user}:{pwd} -T {file_path} ftps://{host}/{directory}/{file_name}'''
        process = subprocess.run(curl_command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)    



def download_csv_from_ftp_as_dataframe(host, user, pwd, directory, file_name, encoding='utf-8', sep=',', ssl=True):
    '''
    Downloads a CSV file from an FTP server and loads it into a pandas DataFrame.

    This function uses curl to download a CSV file from an FTP server. The file is then read into a pandas DataFrame. The FTP server connection details are specified via environment variables.

    Parameters:
    - directory (str): The directory on the FTP server where the file is located.
    - file_name (str): The name of the file to download.

    Returns:
    pandas.DataFrame: A DataFrame containing the data from the downloaded CSV file.
    '''

    # Create a temporary file to download the CSV to
    with tempfile.NamedTemporaryFile(mode='w+', delete=True) as tmpfile:
        csv_file_path = tmpfile.name

        # Prepare FTP connection details, escaping special characters
        https_proxy = os.getenv('https_proxy').replace('!', r'\!').replace('$', r'\\\$')

        # Execute the curl command to download the file
        if ssl:
            curl_command = f'curl -v -x {https_proxy} --ssl -u {user}:{pwd} -o {csv_file_path} ftps://{host}/{directory}/{file_name}'
        else:
            curl_command = f'curl -v -x {https_proxy} -u {user}:{pwd} -o {csv_file_path} ftp://{host}/{directory}/{file_name}'

        process = subprocess.run(curl_command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

        # Read the downloaded CSV file into a pandas DataFrame
        df = pd.read_csv(csv_file_path, dtype=str, keep_default_na=False, encoding=encoding, sep=sep)
        
        return df
    
def download_json_data(url, params=None):
    '''
    Downloads JSON data from a specified URL using a GET request.

    This function sends a GET request to a specified URL to download JSON data. The request includes headers for accepting JSON and an authorization token, which is specified via environment variables. Proxies are also used if specified in environment variables.

    Parameters:
    - url (str): The URL from which to download the JSON data.
    - params (dict, optional): The parameters to include in the request. Defaults to None.

    Returns:
    dict: A dictionary containing the downloaded JSON data.
    '''
    
    response = requests.get(url, headers=get_default_headers(), proxies=get_default_proxies(), params=params)
    return response.json()
    

#############################################
###### Quality assert & Msgs. Helpers #######
#############################################
    
def get_outer_join(df1, df2 ,cols_to_ignore=[]):
    '''
    Asserts whether two pandas DataFrames are equal, ignoring specified columns.

    This function compares two DataFrames, optionally ignoring specified columns, to determine if they are equal. Equality is defined as having the same content in all cells, disregarding the order of rows and columns, after converting all data to string type for comparison.

    Parameters:
    - df1 (pandas.DataFrame): The first DataFrame to compare.
    - df2 (pandas.DataFrame): The second DataFrame to compare.
    - cols_to_ignore (list of str, optional): Columns to ignore during the comparison. Defaults to an empty list.

    Returns:
    bool: True if the DataFrames are considered equal (after ignoring specified columns), False otherwise.
    '''

    # Drop specified columns to ignore from both DataFrames
    if len(cols_to_ignore) > 0:
        for col in cols_to_ignore:
            if col in df1.columns:
                df1 = df1.drop(cols_to_ignore, axis=1)
            if col in df2.columns:
                df2 = df2.drop(cols_to_ignore, axis=1)

    # Convert all data in both DataFrames to string type for comparison
    df1 = df1.astype(str)
    df2 = df2.astype(str)
    
    # Attempt to merge both DataFrames to find discrepancies
    try:
        comp = pd.merge(df1, df2, how='outer', indicator=True)
    except:
        # Return right dataframe if no match was possible
        df2['_merge'] == 'Neu'
        return df2
    
    # Return all exclusive dataframes
    comp['_merge'] = comp['_merge'].astype(str)
    comp =  comp[(comp['_merge'] == 'right_only') | (comp['_merge'] == 'left_only')]
    comp.loc[comp['_merge'] == 'right_only', '_merge'] = 'Neu'
    comp.loc[comp['_merge'] == 'left_only', '_merge'] = 'Entfernt'

    return comp


def send_digest(filename, df, email_list, comment = '-'):
    '''
    Sends an email digest summarizing the differences between two versions of a DataFrame.

    This function generates a digest summarizing the differences between an old and a new version of a DataFrame, including the number of rows added or removed. It then sends this summary via email to a list of recipients.

    Parameters:
    - filename (str): The name of the file associated with the DataFrame, to be mentioned in the email.
    - df_old (pandas.DataFrame): The old version of the DataFrame.
    - df_new (pandas.DataFrame): The new version of the DataFrame.
    - email_list (list of str): A list of email addresses to send the digest to.
    - comment (str, optional): A comment or note to include in the email. Defaults to '-'.

    Returns:
    None
    '''
    subject = f'Airflow Digest - {filename}'

    # Get change metrics
    added_rows = len(df[(df['_merge'] == 'Neu')])
    removed_rows = len(df[(df['_merge'] == 'Entfernt')])

    # Generate HTML content summarizing the differences
    html_content = f'''
        Die Datei {filename} hat sich verändert.<br><br>
        Anzahl neue Zeilen: <b>{added_rows}</b><br>
        Anzahl entfernte Zeilen: <b>{removed_rows}</b><br><br>
        Kommentar: <b>{comment}</b><br><br>
    '''

    # Attach added and deleted rows to the email, if they are few enough to display directly
    # If there are too many, note that they are available as attachments
    if len(df) > 50:
        html_content += 'Änderungen: <b>Siehe Anhang</b><br><br>'
    else:
        html_content += f'Änderungen:<br>{df.to_html(index=False)}'

    # Use temporary file storage to create csvs of added and deleted rows. Then attach them to the mail
    with tempfile.NamedTemporaryFile(mode='w', delete=True, prefix='aenderungen') as temp_file_add:
        df_path = temp_file_add.name + '.csv'
        df.to_csv(df_path, index=False, sep=';')

        # Send mail to all recipients
        for mail in email_list:
            send_email(
                    to=mail,
                    subject=subject,
                    html_content = html_content,
                    files=[df_path]
            )             


def update_without_digest(host, user, pwd, directory, file_name, df):
    '''
    Updates a file on an FTP server without sending a digest email.

    This function saves a pandas DataFrame as a CSV file on an FTP server using the provided connection details. Unlike `update_with_digest`, this function does not compare the new DataFrame with an existing file or send a digest email summarizing any changes.

    Parameters:
    - host (str): The hostname of the FTP server.
    - user (str): The username for authentication.
    - pwd (str): The password for authentication.
    - directory (str): The directory on the FTP server where the file should be saved.
    - file_name (str): The name of the file to be saved on the FTP server.
    - df (pandas.DataFrame): The DataFrame to save as a CSV file.

    Returns:
    None
    '''
    save_pandas_to_csv_on_ftp(df, host, user, pwd, directory, file_name)


def update_with_digest(host, user, pwd, directory, file_name, df, comment='-', cols_to_ignore_for_digest=[]):
    '''
    Updates a file on an FTP server with a new DataFrame and sends a digest email summarizing any changes.

    This function first checks if the new DataFrame differs from the existing file on the FTP server. If there are differences, it updates the file on the server and sends a digest email to a list of recipients summarizing the changes.

    Parameters:
    - directory (str): The directory on the FTP server where the file is located.
    - file_name (str): The name of the file to update.
    - df (pandas.DataFrame): The new DataFrame to upload.
    - comment (str, optional): A comment to include in the digest email. Defaults to '-'.
    - cols_to_ignore_for_digest (list of str, optional): Columns to ignore when compiling the digest mail
 
    Returns:
    None
    '''

    # Attempt to download the old DataFrame from the FTP server
    try:
        df_old = download_csv_from_ftp_as_dataframe(host, user, pwd, directory, file_name)
    except:
        df_old = pd.DataFrame(columns=df.columns)

    df_comp = get_outer_join(df_old, df)

    if len(df_comp) > 0:
        save_pandas_to_csv_on_ftp(df, host, user, pwd, directory, file_name)

    df_comp = get_outer_join(df_old, df, cols_to_ignore_for_digest)

    if len(df_comp) > 0:
        send_digest(file_name, df_comp, os.getenv('ogd_alert_mails').split(','), comment)    
    

###################################
######### Boilerplate helpers #####
###################################

def get_default_proxies():
    return {
        'http': os.getenv("https_proxy"),
        'https' : os.getenv("https_proxy")
        }

def get_default_headers():
    return {
        'Accept': 'ext/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
        }

def download_table_as_df(conn_string, query):
    '''
    Downloads data from a PostgreSQL database table and loads it into a pandas DataFrame.
    
    Parameters:
    - conn_string (str): The connection string for the PostgreSQL database.
    - query (str): The SQL query to execute.

    Returns:
    pandas.DataFrame: A DataFrame containing the data fetched by executing the SQL query.  
    '''
    with psycopg2.connect(conn_string) as conn:
        df = pd.read_sql_query(query, conn)
    
    return df 