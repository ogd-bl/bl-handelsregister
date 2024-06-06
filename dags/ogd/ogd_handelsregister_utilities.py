import requests
import pandas as pd
from datetime import datetime, timedelta
from rapidfuzz.distance import DamerauLevenshtein
import time
import pyproj
from sqlalchemy import create_engine, text
import os
import numpy as np
import re
from io import BytesIO, StringIO
import json

from dotenv import load_dotenv

##########################################
###### Configuration and Constants #######
##########################################

# Load environment variables from .env file
load_dotenv()

# Load config parameters from config file
config_file = 'ogd_handelsregister_config.json'
config_file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), config_file) # Make sure that the relatively placed file is found by constructing the absolute path

with open(config_file_path, 'r') as file: # Read config file
    config = json.load(file)

# Create coordinate transformation objects
wg84_lv95_transformer = pyproj.Transformer.from_crs(config['WG84_EPSG'], config['LV95_EPSG'])
lv95_wg84_transformer = pyproj.Transformer.from_crs(config['LV95_EPSG'], config['WG84_EPSG'])

# Database connection
database_url = f"postgresql://{os.getenv('ogd_analytics_user')}:{os.getenv('ogd_analytics_password')}@{os.getenv('ogd_analytics_host')}/{os.getenv('ogd_analytics_db')}"
engine = create_engine(database_url)


##########################################
############## Pipelines #################
##########################################

def squash_and_update_companies_inventory(session_zefix):
    '''
    This function serves as a complete pipeline for fetching, processing, and storing updated company
    information of a canton in the database. Herefore the database is truncated and replaced with the current snapshot of
    all companies in the canton.

    Attention: Some settings of this function are set in the config file

    The process involves:
    - Downloading all companies located in the canton.
    - Geolocating and transforming the company data.
    - Squashing the data (if needed) and uploading it to the database.

    Parameters:
    - session_zefix: Session object for the connection to the Zefix API
    - canton: Abbreviation of the canton (e.g. 'BL') to download the company data for
    '''
    # Download additional data used later
    legal_form_mappings = download_legal_form_mappings()
    district_mappings = download_district_data()

    # Download UIDs and detail information of all companies currently located in the canton
    df = download_all_companies(session_zefix, config['CANTON_ABBREVIATION'])

    # Add geoinformation to the company data
    df = geolocate_companies(df)

    # Transform structure of dataframe and add information
    df = transform_to_inventory_db_structure(df, legal_form_mappings, district_mappings)

    # Replace db table
    replace_inventory_database(df)


def update_companies_inventory(start_date, end_date, session_zefix):
    '''
    Updates companies' data in the database for a given date range within a specific canton.
    This function serves as a complete pipeline for fetching, processing, and storing updated company
    information in the database.
    The function removes all uids from the database that were updated during the date range. It then
    adds entries to the database for all companies inside the canton that were updated. In this
    way we can make sure to remove all companies if they moved out of the canton while updating all
    companies that moved into the canton or changed in some other way.

    Attention: Some settings of this function are set in the config file

    The process involves:
    - Downloading updated companies' data between `start_date` and `end_date`.
    - Downloading updated companies' data for companies with entries in the address correction db
    - Geolocating and transforming the updated company data.
    - Updating the database with the new information while removing existing records of the updated companies.

    Parameters:
    - start_date: The start date for fetching updated companies' data.
    - end_date: The end date for fetching updated companies' data.
    - session_zefix: Session object for the connection to the Zefix API
    - canton: Abbreviation of the canton (e.g. 'BL') to download the company data for

    Returns:
    - df_deleted: A pandas dataframe with the rows removed from the table
    - df_inserted: A pandas dataframe with the rows added to the table
    '''
    # Download additional data used later
    legal_form_mappings = download_legal_form_mappings()
    district_mappings = download_district_data()

    # Get all companies' uids currently residing in the canton
    df_canton = download_company_uids_by_canton(session_zefix, config['CANTON_ABBREVIATION']) 

    # Get all companies' uids mentioned in an announcement during the specified time period
    df_updated = download_updated_companies(start_date, end_date, session_zefix) 

    # Get all companies that have cleansed addresses in our db
    df_cleansed = download_cleansed_addresses() 

    # The uids to be removed from the db are all updated companies and all companies with cleansed addresses
    uids_to_remove = df_updated['uid'].tolist() + df_cleansed['uid'].tolist()
    uids_to_remove = list(set(uids_to_remove))

    # The companies to be inserted are all companies that i) were updated, and ii) are still located inside the canton, and iii) are still ACTIVE or BEING_CANCELLED
    df_updated = df_updated[df_updated['uid'].isin(df_canton['uid'])]
    df_cleansed = df_cleansed[df_cleansed['uid'].isin(df_canton['uid'])] # This is important because the address correction db might be out of date
    df_updated = df_updated[df_updated['status'].isin(['BEING_CANCELLED', 'ACTIVE'])]

    uids_to_update = df_updated['uid'].tolist() + df_cleansed['uid'].tolist()
    uids_to_update = list(set(uids_to_update))

    # Abbord if no companies have to be updated or to be removed from the table
    if len(uids_to_update) == 0 and len(uids_to_remove) == 0:
        return pd.DataFrame(), pd.DataFrame()

    else:
        # Download all details for companies to insert
        df = download_zefix_company_details(session_zefix, uids_to_update)
        
        # Geolocate
        df = geolocate_companies(df)

        # Transform structure of dataframe and add additional information
        df = transform_to_inventory_db_structure(df, legal_form_mappings, district_mappings)

        # Remove and update company entries
        df_deleted, df_inserted = update_inventory_database(uids_to_remove, df)

        return df_deleted, df_inserted


def update_sogc_messages(start_date, end_date, session_zefix):
    '''
    Updates company mutations and relocations for a specific canton during a specified time range in our db.

    Attention: Some settings of this function are set in the config file

    The process involves:
    - Downloading all sogc messages from ZEFIX during the specified time range
    - Creating two dataframes for the mutations and the relocations
    - Adding additional fields such as noga information and extracting source and target cantons of relocations
    - Filtering the dataframes based on the specified canton (specified in the config file)
    - Updating the tables in our database

    Parameters:
    - start_date: The start date for fetching updated companies' data.
    - end_date: The end date for fetching updated companies' data.
    - session_zefix: Session object for the connection to the Zefix API
    - canton: Abbreviation of the canton (e.g. 'BL') to download the company data for

    Returns:
    - df_mut: A pandas dataframe with the newly added rows of the company mutation table
    - df_reloc: A pandas dataframe with the newly added rows of the company relocation table
    '''
    # Download Daily sogc Publications during the specified time range
    df = download_sogc_publications(session_zefix, start_date, end_date)
    if len(df) == 0:
        return None, None

    # Download necessary data for later stages
    munic_mappings = download_municipality_mappings()
    cant_mappings = download_canton_mappings()
    legal_form_mappings = download_legal_form_mappings()
    canton_name_shorts = download_canton_code_mappings()


    # Format the timestamp columns
    df['sogcDate'] = pd.to_datetime(df['sogcDate'], format='%Y-%m-%d')
    df['registryOfCommerceJournalDate'] = pd.to_datetime(df['registryOfCommerceJournalDate'], format='%Y-%m-%d')

    # Create dataframe for mutations
    df_mut = process_mutations(df, munic_mappings, legal_form_mappings)

    # Update table in database
    update_mutation_database(df_mut)

    # Create dataframe for relocations
    df_reloc = process_relocations(df, munic_mappings, cant_mappings, legal_form_mappings, canton_name_shorts)

    # Update table in database
    update_relocation_database(df_reloc)

    return df_mut, df_reloc


##########################################
############# Download Helpers ###########
##########################################

def download_updated_companies(start_date, end_date, session_zefix):
    '''
    Downloads the details of companies that have been updated within a specified date range and canton.

    Attention: Some settings of this function are set in the config file

    This function performs several steps:
    - Establishes a session with the Zefix (Central Business Names Index of Switzerland) API.
    - Uses the session to download a list of updates (e.g., new registrations, changes, deletions) from the SOGC (Swiss Official Gazette of Commerce) for the given date range.
    - Filters the updates for those relevant to the specified canton.
    - Downloads detailed company information for each updated company in the specified canton.

    Parameters:
    - start_date (str): The start date for fetching updates in 'YYYY-MM-DD' format.
    - end_date (str): The end date for fetching updates in 'YYYY-MM-DD' format.
    - session_zefix: Session object for the connection to the Zefix API

    Returns:
    - df (pandas.DataFrame): A DataFrame containing detailed information about each updated company.
    '''

    # Download all messages during the time range from ZEFIX
    df_updates = download_sogc_publications(session_zefix, start_date, end_date)

    # Return an empty dataframe if no messages were published during the time range
    if len(df_updates) == 0:
        return pd.DataFrame(columns=['uid', 'status'])

    return df_updates


def download_sogc_publications(session_zefix, start_date, end_date):
    '''
    Downloads the SOGC publications for a specified date range. If records are found, it combines specific
    publication data with company short information into a DataFrame.

    Attention: Some settings of this function are set in the config file

    Parameters:
    - session_zefix: Session object for the connection to the Zefix API
    - start_date (str): The start date for fetching updates in 'YYYY-MM-DD' format.
    - end_date (str): The end date for fetching updates in 'YYYY-MM-DD' format.

    Returns:
    - df (Pandas dataframe): A pandas dataframe with one row per message
    '''

    # Placeholder for records
    records = []

    # Downlaod all messages during the time period
    while start_date < end_date:

        url = config['BASE_URL_SOGC'] + start_date.strftime('%Y-%m-%d')

        # Fetch the records
        rec = session_zefix.get(url).json()

        # The API recurns a list of two dictionaries. We combine them into one dictionary in order to create one row in the dataframe afterwards
        if isinstance(rec, list):

            # Combine dictionaires
            rec = [{**record['sogcPublication'], **record['companyShort']} for record in rec]

            # Add the publication date to the entry as this is not returned by the api
            for i in range(len(rec)):
                rec[i]['publication_date'] = start_date
            
            # Add records to placeholder
            records += rec
        else:

            # If the api has no records for the specified date it returns a non-list object. We don't specifically handle these cases but merely print a short message
            print('Error', start_date)

        start_date += timedelta(days=1)

    # Generate dataframe. Each record contains two keys with dictionaries. They are flattened into one row
    if len(records) > 0:
        return pd.DataFrame.from_dict(records)
    else:
        return pd.DataFrame()


def download_all_companies(session_zefix, canton):
    '''
    Download detail information of all companies registered in a specified canton.

    Parameters:
    - session_zefix: Session object for the connection to the Zefix API
    - canton: Abbreviation of the canton (e.g. 'BL') to download the company data for

    Returns:
    - df: A pandas dataframe
    '''

    # Download companiy UIDs by canton
    df = download_company_uids_by_canton(session_zefix, canton)

    # Download detailed company information
    df = download_zefix_company_details(session_zefix, df['uid'])

    return df


def download_company_uids_by_canton(session, canton, active=True):
    '''
    Downloads uids of companies registered in a specified canton. It fetches all the uids by community in the canton to prevent API failures.

    Attention: Some settings of this function are set in the config file

    Parameters:
    - session: A session object from the requests library, configured with necessary authentication.
    - canton: The canton abbreviation (e.g., 'ZH' for Zurich) from which to download company data.
    - active: A boolean flag to filter companies by their active status.
    
    Returns: 
    - df: A pandas DataFrame containing the companies from the specified canton.
    '''

    # Cet all communities' identifiers from ZEFIX
    communities = session.get(config['ZEFIX_COMMUNITIES_ENDPOINT']).json()

    # Placeholder for the records
    records = []

    # Iterate over each community within the specified canton and each legal form
    for community in [community['bfsId'] for community in communities if community['canton'] == canton]:

        # Create body for post request
        body = {
            'legalSeatId': community,
            'activeOnly': active
            }

        # Fetch data
        response = session.post(config['BASE_URL_SEARCH'], json = body)

        records += response.json()

    # Create data frame from all fetched records
    df = pd.DataFrame(data = records, dtype=str)

    return df


def download_zefix_company_details(session, uid_list):
    '''
    Downloads detailed information for a list of companies identified by their UIDs.

    Attention: Some settings of this function are set in the config file

    Parameters:
    - session: A session object from the requests library, configured with necessary authentication.
    - uid_list: A list of company UIDs for which to download detailed information.
    
    Returns:
    - df: A pandas DataFrame containing detailed information for the specified companies.
    '''

    # Placeholder for records
    records = []

    # Download company information for each of the provided uids
    for uid in uid_list:
        url = config['BASE_URL_COMPANY'] + str(uid)
        response = session.get(url).json()
        records += response

    # Create dataframe from records
    return pd.DataFrame.from_records(records)


def download_noga_codes(df, uid_col, noga_col):
    '''
    Download NOGA codes for companies based on their uid.

    Parameters:
    - df: DataFrame containing company data.
    - uid_col (string): name of the column in df that contains the uids.
    - noga_col (string): name of the column where NOGA codes will be stored.

    Returns:
    - df: The original dataframe with a new column containing the noga codes
    '''

    # Download the noga codes and add them to the dataframe
    with requests.Session() as session_burweb:

        session_burweb.auth = (os.getenv('ogd_burwerb_user'), os.getenv('ogd_burwerb_password'))

        # Populate the specified column with NOGA codes
        df[noga_col] = download_noga_code(session_burweb, df[uid_col])

    return df


def download_noga_code(session, uid_list):
    '''
    Downloads NOGA codes for a list of companies identified by their UIDs.

    Attention: Some settings of this function are set in the config file

    Parameters:
    - session: A session object from the requests library, configured with necessary authentication.
    - uid_list: A list of company UIDs for which to download NOGA codes.

    Returns:
    noga_codes: A list of NOGA codes corresponding to the provided UIDs.
    '''

    # A dictionary as intermediary result that matches uids to noga codes
    # We do this to prevent the code from downloading the same noga code for identical uids multiple times
    noga_mappings = {}

    # For each unique uid in the list, download the noga code
    for uid in list(set(uid_list)):

        # Placeholder
        noga_code = None

        # The api sometimes has hiccups. We try a maximum of three times to download the noga code bevore abording
        max_tries = 3
        n_try = 1

        while n_try <= max_tries:
            try:
                url = f'{config["BURWEB_BASE_URL"]}{uid}/NogaCode'
                noga_code = session.get(url).json()['NogaCode']
                break
            except:
                time.sleep(1)
                n_try += 1

        # Add uid -> noga_code mapping to dictionary
        noga_mappings[uid] = noga_code

    # Build a list that mapps a noga code to all uids in the provided list
    return [noga_mappings[uid] for uid in uid_list]


def download_kgwr_data():
    '''
    Retrieve KGWR (Kataster der öffentlich-rechtlichen Geoinformation des Kantons Basel-Landschaft) address data.

    Attention: Some settings of this function are set in the config file
    
    Returns:
    - df: DataFrame containing KGWR address data with selected columns. All columns are strings.
    '''

    # Read data from ODS table
    df_kgwr = pd.read_parquet(BytesIO(requests.get(config['KGWR_ADRESSEN'], proxies=get_default_proxies()).content))

    # Filter necessary columns
    df_kgwr = df_kgwr[['strassenbezeichnung', 'eingangsnummer_gebaeude', 'postleitzahl', 'gemeindename', 'egid', 'e_eingangskoordinate', 'n_eingangskoordinate']]

    # Turn all columns into strings
    df_kgwr = df_kgwr.astype(str)

    return df_kgwr


def download_district_data():
    '''
    Retrieve district data including BFS numbers, district IDs, and names.

    Attention: Some settings of this function are set in the config file

    Returns:
    - dict: Dictionary mapping BFS numbers to their respective district IDs and names.
    '''

    # Download disctrict data as dataframe
    df_raum = pd.read_csv(BytesIO(requests.get(config['DISTRICT_DATA_LINK'], proxies=get_default_proxies()).content), sep=';', dtype=str)

    # Iterate through the DataFrame to populate the district mappings dictionary
    district_mappings = {}

    for _, row in df_raum.iterrows():
        district_mappings[row['BFS_Nummer']] = {'bezirk_id': str(row['Bezirk_Nummer']), 'bezirk_name': row['Bezirk']}

    return district_mappings


def download_cleansed_addresses():
    '''
    Retrieve cleansed addresses from the database.

    Returns:
    - df: DataFrame containing the cleansed addresses.
    '''
    with engine.connect() as connection, connection.begin():
        schema = config['DB_SCHEMA']
        table = config['ADDRESS_CORRECTIONS_TABLE']
        return pd.read_sql_query(f'SELECT * FROM {schema}.{table}', connection)


##########################################
########### Geolocating Helpers ##########
##########################################

def geolocate_companies(df):
    '''
    Geolocates companies in a dataframe. And returns the enriched dataframe.

    This involves several steps:
    - Downloading additional data required for geolocation and transformation.
    - Extracting and preparing specific address components from the DataFrame.
    - Performing geolocation using multiple strategies (exact match with gwr, fuzzy match, and external service).
    - Transforming the DataFrame into a format suitable for uploading to a specific database schema.

    Attention: Some settings of this function are set in the config file

    Parameters:
    - df: A pandas DataFrame containing companies' data to be geolocated and transformed.

    Returns:
    - A transformed pandas DataFrame with geolocated addresses and additional information ready for database insertion.
    '''

    # Abboard if df is empty
    if len(df) == 0:
        return df

    # Download cantonal building information for exact matching of the addresses
    df_kgwr = download_kgwr_data()

    # Download address corrections from our db
    df_clean = download_cleansed_addresses()

    # Get address information from the 'address' column that is a dictionary
    df['strassenbezeichnung'] = df['address'].apply(lambda x: x['street'])
    df['eingangsnummer_gebaeude'] = df['address'].apply(lambda x: x['houseNumber'])
    df['postleitzahl'] = df['address'].apply(lambda x: x['swissZipCode'])
    df['ort'] = df['address'].apply(lambda x: x['city'])

    # Specify columns to extract from the gwr data during geolocation
    extract_cols = ['strassenbezeichnung', 'eingangsnummer_gebaeude', 'postleitzahl', 'gemeindename', 'egid', 'e_eingangskoordinate', 'n_eingangskoordinate']

    # Initialize all geolocation columns with None
    for col in [col + '_loc' for col in extract_cols]:
        df[col] = [None] * len(df)

    df['matching_style'] = None
    df['comment'] = None

    # Specify columns upon which the matching is done
    match_cols = ['strassenbezeichnung', 'eingangsnummer_gebaeude', 'postleitzahl']

    # Try a first exact match with the gwr data
    df = locate_using_kgwr(df, df_kgwr, match_cols, extract_cols, 'exact kgwr match')
    
    # Add address correction data from our db to the dataframe
    df = correct_addresses(df, df_clean, ['strassenbezeichnung', 'eingangsnummer_gebaeude', 'postleitzahl'])

    # Try to match the addresses again after cleansing
    # This is done in this order so that if an entry in the address correction db is outdated, its address is not wrongfully corrected before matching
    df = locate_using_kgwr(df, df_kgwr, match_cols, extract_cols, 'kgwr match with replaced address')

    # Try to match the addresses with simplyfied house numbers
    df = locate_using_kgwr_simple_number(df, df_kgwr, match_cols, extract_cols, 'kgwr match with adapted house number')

    # Try to match the addresses with fuzzy matching on the street name
    df = locate_using_fuzzy_matching(df, df_kgwr, match_cols, extract_cols, 'strassenbezeichnung', 1,  'kgwr match with adapted street name')

    # Use Nominatim geolocation for not matched companies
    # df = locate_using_nominatim(df, config['CANTON'], extract_cols)

    return df


def locate_using_kgwr(df, df_kgwr, match_cols, extract_cols, location_style_label):
    '''
    Match addresses using KGWR data to locate companies.

    Parameters:
    - df: DataFrame containing company data that needs address matching.
    - df_kgwr: DataFrame with KGWR address data.
    - match_cols: List of columns used for matching addresses between `df` and `df_kgwr`.
    - extract_cols: List of columns from `df_kgwr` to add to `df` after a successful match.

    Returns:
    - df: DataFrame with updated address information based on KGWR data.
    '''

    # Perform the matching operation and extract relevant columns for matched addresses
    matches = kgwr_match(df[df['matching_style'].isnull()], df_kgwr, match_cols, extract_cols)

    # Update the original DataFrame with the matched address information
    for col in [col for col in extract_cols]:
        df.loc[df['matching_style'].isnull(), col + '_loc'] = matches[col].to_list()

    # Mark the matching style as 'exact kgwr match' for successfully matched addresses
    df.loc[df['matching_style'].isnull() & ~df['e_eingangskoordinate_loc'].isnull(), 'matching_style'] = location_style_label

    return df

def locate_using_kgwr_simple_number(df, df_kgwr, match_cols, extract_cols, location_style_label):
    '''
    Match addresses using KGWR data to locate companies but with simplified house numbers. The simplified
    housenumbers are always the first occurence of one or more digits (e.g. 90a -> 90 or 40-42 -> 40)

    Parameters:
    - df: DataFrame containing company data that needs address matching.
    - df_kgwr: DataFrame with KGWR address data.
    - match_cols: List of columns used for matching addresses between `df` and `df_kgwr`.
    - extract_cols: List of columns from `df_kgwr` to add to `df` after a successful match.

    Returns:
    - df: DataFrame with updated address information based on KGWR data.
    '''

    # Store original house number in new column
    df['eingangsnummer_gebaeude_original'] = df['eingangsnummer_gebaeude']

    # Extract first occurence of one or more digits from house number
    df['eingangsnummer_gebaeude'] = df['eingangsnummer_gebaeude'].str.extract(r'(\d+)')

    # Extract first occurence of one or more digits from house number
    df_kgwr['eingangsnummer_gebaeude'] = df_kgwr['eingangsnummer_gebaeude'].str.extract(r'(\d+)')

    # Perform the matching operation and extract relevant columns for matched addresses
    matches = kgwr_match(df[df['matching_style'].isnull()], df_kgwr, match_cols, extract_cols)

    # Update the original DataFrame with the matched address information
    for col in [col for col in extract_cols]:
        df.loc[df['matching_style'].isnull(), col + '_loc'] = matches[col].to_list()

    # Mark the matching style as 'exact kgwr match' for successfully matched addresses
    df.loc[df['matching_style'].isnull() & ~df['e_eingangskoordinate_loc'].isnull(), 'matching_style'] = location_style_label

    # Replace simplified house number with original house number for further usage
    df.loc[df['matching_style'] == location_style_label, 'eingangsnummer_gebaeude'] = df.loc[df['matching_style'] == location_style_label, 'eingangsnummer_gebaeude_original']

    return df


def kgwr_match(df, df_kgwr, match_cols, extract_cols):
    '''
    Matches addresses from a DataFrame against KGWR address data, using exact matching criteria.

    Parameters:
    - df: DataFrame containing the addresses to match.
    - df_kgwr: DataFrame containing KGWR address data.
    - match_cols: Columns to be used for matching between the two DataFrames.
    - extract_cols: Columns to extract from df_kgwr upon a successful match.

    Returns:
    - df: A DataFrame with extracted columns for matched addresses.
    '''

    # Weed out columns of dataframes to relevant ones
    df = df[match_cols]
    df_kgwr_lookup = df_kgwr[match_cols]

    # Ensure matching is case insensitive by converting all relevant fields to lowercase
    # We do this now so it is only done once and not multiple times later in the loop
    for col in match_cols:
        df.loc[:,col] = df[col].astype(str).str.lower()
        df_kgwr_lookup.loc[:,col] = df_kgwr_lookup[col].astype(str).str.lower()

    # Initialize results DataFrame
    results = pd.DataFrame(columns=extract_cols)

    # Iterate through df to find matches in df_kgwr
    for _, row in df.iterrows():
        # Check row-wise equality, select rows where all columns match
        matches = df_kgwr_lookup[match_cols].eq(row, axis='columns').all(axis=1)
        matches = df_kgwr[matches]

        # Add resulting row to results DataFrame. If nothinig was matched, the row conatins nans
        if len(matches) > 0:
            results.loc[len(results)] = matches.iloc[0][extract_cols]
        else:
            results.loc[len(results)] = [np.nan]*len(extract_cols)

    # We used nans before to prevent full None-columns. For further processing we use Nones
    results.replace(np.nan, None, inplace=True)

    return results

def locate_using_fuzzy_matching(df, df_kgwr, match_cols, extract_cols, fuzzy_col, max_dist, location_style_label):
    '''
    Locate addresses using fuzzy matching to improve accuracy when exact matches are not possible.

    Parameters:
    - df: DataFrame containing company data that needs address matching.
    - df_kgwr: DataFrame with KGWR address data for matching.
    - match_cols: List of columns used for matching addresses between `df` and `df_kgwr`.
    - extract_cols: List of columns from `df_kgwr` to be added to `df` after a successful match.
    - fuzzy_col: The specific column in `df` to apply fuzzy matching on.
    - max_dist: The maximum damerau-levenshtein distance for a match to be considered valid

    Returns:
    - df: DataFrame with updated address information based on fuzzy matching results.
    '''

    # Perform fuzzy matching and retrieve matches and match information
    matches, match_info = fuzzy_match(df[df['matching_style'].isnull()], df_kgwr, match_cols, extract_cols, fuzzy_col, max_dist)

    # Update the original DataFrame with matched address information and comments on the match
    for col in [col for col in extract_cols]:
        df.loc[df['matching_style'].isnull(), col + '_loc'] = matches[col].to_list()

    df.loc[df['matching_style'].isnull(), 'comment'] = match_info

    # Mark the matching style as 'fuzzy kgwr match' for successfully matched addresses
    df.loc[df['matching_style'].isnull() & ~df['e_eingangskoordinate_loc'].isnull(), 'matching_style'] = location_style_label

    return df


def fuzzy_match(df, df_kgwr, match_cols, extract_cols, fuzzy_col, max_dist):
    '''
    Performs fuzzy matching on a specified column between two DataFrames and extracts relevant information upon a successful match.

    Parameters:
    - df: DataFrame containing the data to be matched.
    - df_kgwr: DataFrame containing reference data for matching.
    - match_cols: Columns to be considered for matching.
    - extract_cols: Columns to extract from the reference DataFrame upon a successful match.
    - fuzzy_col: The column on which to perform fuzzy matching.
    - max_dist: Maximum allowable distance for a match to be considered successful.
    
     Returns:
      - results:DataFrame of matched results
      - match_info: list with additional information about the matchings.
    '''

    # Weed out columns to the necessary ones
    df = df[match_cols]
    df_kgwr_lookup = df_kgwr[match_cols]

    # Initialize DataFrame for results
    results = pd.DataFrame(columns=extract_cols)

    # Initialize list to hold information about the matches
    match_info = []

    # Perform fuzzy matching for each row in df
    for _, row in df.iterrows():

        # Get value to match
        fuzz = row[fuzzy_col]

        # Initialize minimum distance and value for fuzzy matching
        min_fuzz_value = None
        min_fuzz_dist = max_dist + 1

        # Iterate over unique values in the fuzzy column of df_kgwr
        for target in df_kgwr[fuzzy_col].unique():
            # Calculate the distance using the Damerau-Levenshtein method
            if fuzz is not None and target is not None:
                dist = DamerauLevenshtein.distance(fuzz.lower(), target.lower())

                # Exit loop if a perfect match is found
                if dist == 0:
                    min_fuzz_value = target
                    min_fuzz_dist = dist
                    break

                # Update minimum distance and value if current distance is lower
                if min_fuzz_value is None or min_fuzz_dist > dist:
                    min_fuzz_value = target
                    min_fuzz_dist = dist

        # Check if match is sufficient based on max_dist
        if min_fuzz_dist > max_dist:
            # Append None values if no match is found
            results.loc[len(results)] = [np.nan]*len(extract_cols)
            match_info.append(None)
        else:
            # Create row object for quick lookup in kgwr data
            tmp_row = row.copy()
            tmp_row[fuzzy_col] = min_fuzz_value

            # Find the matching row in df_kgwr
            matches = df_kgwr_lookup[match_cols].eq(tmp_row, axis='columns').all(axis=1)
            matches = df_kgwr[matches]

            # Extract relevant columns from the first matching row
            if len(matches) > 0:
                results.loc[len(results)] = matches.iloc[0][extract_cols]
                match_info.append(f'{fuzz} -> {min_fuzz_value}')
            else:
                # Append None values if no match is found
                results.loc[len(results)] = [np.nan]*len(extract_cols)
                match_info.append(None)

    # To prevent full None columns we used nan in this function. We convert them to Nones for further calculations
    results.replace(np.nan, None, inplace=True)

    return results, match_info


def locate_using_nominatim(df, canton, extract_cols):
    '''
    Use the Nominatim geocoding service to locate addresses that could not be matched using other methods.

    Attention: Some settings of this function are set in the config file

    Parameters:
    - df: DataFrame containing company data that needs address matching.
    - canton: String specifying the canton, to narrow down the search scope.
    - extract_cols: List of columns to extract from the Nominatim results and add to `df`.

    Returns:
    - df: DataFrame with updated address information based on Nominatim geocoding results.
    '''

    # Columns to be used for the Nominatim search
    search_cols = config['NOMINATIM_SEARCH_COLUMNS']

    # Perform Nominatim matching and retrieve matches and match information
    matches, match_info = nominatim_match(df[df['matching_style'].isnull()], search_cols, canton, extract_cols, fair_use=True)

    # Update the original DataFrame with matched address information and comments on the match
    for col in [col for col in extract_cols]:
        df.loc[df['matching_style'].isnull(), col + '_loc'] = matches[col].to_list()

    # Mark the matching style as 'nominatim' for successfully matched addresses
    df.loc[df['matching_style'].isnull(), 'comment'] = match_info
    df.loc[df['matching_style'].isnull() & ~df['e_eingangskoordinate_loc'].isnull(), 'matching_style'] = 'nominatim'

    return df


def nominatim_match(df, match_cols, area, extract_cols, fair_use = True):
    '''
    Uses the Nominatim geocoding service to match addresses and extract geographic coordinates.

    Parameters:
    - df: DataFrame containing the data to be geocoded.
    - dfmatch_cols: Columns to use for constructing the query to Nominatim.
    - dfarea: The area (e.g., country or city) to restrict the search within.
    - dfextract_cols: Columns to extract from the Nominatim response.
    - dffair_use: Boolean flag to respect Nominatim's fair use policy by adding a delay between requests.
    
    Returns:
    - A DataFrame with geocoding results and a list with match information.
    '''

    # Initialize DataFrame for storing geocoding results.
    results = pd.DataFrame(columns=extract_cols)

    # List to store additional information about each geocoding attempt.
    match_info = []

    # Iterate through each row in the provided DataFrame to geocode addresses.
    for _, row in df.iterrows():

        # Enforce a delay between requests if fair_use is True. See https://operations.osmfoundation.org/policies/nominatim/
        if fair_use:
            time.sleep(1)

        try:
            # Construct the query from the specified address components and the area.
            query = ' '.join([str(row[col]) for col in match_cols]) + ' ' + area

            # Send the query to Nominatim and parse the JSON response.
            location = requests.get(f'https://nominatim.openstreetmap.org/search?q={query}&format=json&addressdetails=1&limit=1&polygon_svg=0').json()

            # Check if any results were returned and if the essential address component is present. Do not use results with lower granularity than node or way
            if len(location) == 0 or row[match_cols[0]] is None or (('osm_type' in location[0]) and (location[0]['osm_type'] not in ['way', 'node'])):
                # If criteria are not reached, add empty row
                res = [np.nan] * len(extract_cols)
                match_info.append(None)

            else:
                # If a valid location is found, extract the requested information.
                res = []
                for col in extract_cols:
                    if col == 'strassenbezeichnung' and 'address' in location[0] and 'road' in location[0]['address']:
                        res.append(location[0]['address']['road'])

                    elif col == 'eingangsnummer_gebaeude' and 'address' in location[0] and 'house_number' in location[0]['address']:
                        res.append(location[0]['address']['house_number'])

                    elif col == 'postleitzahl' and 'address' in location[0] and 'postleitzahl' in location[0]['address']:
                        res.append(location[0]['address']['postleitzahl'])

                    elif col == 'gemeindename' and 'address' in location[0] and 'village' in location[0]['address']:
                        res.append(location[0]['address']['village'])

                    elif col == 'e_eingangskoordinate' and 'lat' in location[0] and 'lon' in location[0]:
                        res.append(wg84_lv95_transformer.transform(location[0]['lat'], location[0]['lon'])[0])

                    elif col == 'n_eingangskoordinate' and 'lat' in location[0] and 'lon' in location[0]:
                        res.append(wg84_lv95_transformer.transform(location[0]['lat'], location[0]['lon'])[1])

                    else:
                        res.append(None)

                # Append detailed match information for review or debugging.
                match_info.append({'osm_type': location[0]['osm_type'], 'display_name': location[0]['display_name']})
        except:
            # In case of any error during the request or processing, log the exception and append None values.
            res = [np.nan] * len(extract_cols)
            match_info.append(None)

        # Add the processed row to the results DataFrame.
        results.loc[len(results)] = res

    # To prevent fully None rows, we used NaN in this function. For further calculations we use Nones
    results.replace(np.nan, None, inplace=True)

    return results, match_info


#########################################################
########## Helpber Functions for Inventory Process ######
#########################################################

def get_noga_information_from_code_by_level(noga_codes, level):
    '''
    Retrieves hierarchical NOGA information for a list of NOGA codes based on the specified level of detail.

    Attention: Some settings of this function are set in the config file

    Parameters:
    - noga_codes: A list of NOGA codes for which to retrieve detailed information.
    - level: A string specifying the level of detail required ('abschnitt_code', 'abschnitt', 'abteilung', 'gruppe', 'klasse', 'art').

    Returns:
    - A list of detailed NOGA information corresponding to each NOGA code in the list, matched at the specified level.

    Raises:
    - Exception: If an invalid level is specified.
    '''

    # For each level, process the NOGA codes differently based on the level of detail required
    if level == 'abschnitt_code':
        # Load NOGA division to section mappings and standardize codes
        abschnitt_abteilung_match = pd.read_csv(config['NOGA_ABTEILUNG_LOOKUP_LINK'], sep=',')
        abschnitt_abteilung_match['Code'] = abschnitt_abteilung_match['Code'].apply(lambda x: str(x) if len(str(x)) == 2 else '0'+str(x))

        # Map each NOGA code to its corresponding parent section
        vals = {}
        for _, row in abschnitt_abteilung_match.iterrows():
            vals[row['Code']] = row['Parent']

        # Return the parent section for each NOGA code
        return [vals[str(code)[:2]] if str(code)[:2] in vals else None for code in noga_codes]

    if level == 'abschnitt':
        # This block retrieves the name of the section (Abschnitt) each NOGA code belongs to
        # It involves matching NOGA codes to their sections and then retrieving the section names
        abschnitt_abteilung_match = pd.read_csv(config['NOGA_ABTEILUNG_LOOKUP_LINK'], sep=',')
        noga_abschnitt = pd.read_csv(config['NOGA_ABSCHNITT_LINK'], sep=',')
        abschnitt_abteilung_match['Code'] = abschnitt_abteilung_match['Code'].apply(lambda x: str(x) if len(str(x)) == 2 else '0'+str(x))

        # Create mappings from NOGA codes to section IDs and then to section names
        vals_1 = {}
        for _, row in abschnitt_abteilung_match.iterrows():
            vals_1[row['Code']] = row['Parent']

        vals_2 = {}
        for _, row in noga_abschnitt.iterrows():
            vals_2[row['Code']] = row['Name_de']

        # Return the section name for each NOGA code
        return [vals_2[vals_1[str(code)[:2]]] if str(code)[:2] in vals_1 else None for code in noga_codes]

    if level == 'abteilung':
        # Directly retrieves the division (Abteilung) names for the given NOGA codes
        noga_abteilung = requests.get(config['NOGA_ABTEILUNG_LINK']).json()

        # Map each NOGA code to its corresponding name
        vals = {}
        for entry in noga_abteilung['concept']['codeListEntries']:
            vals[entry['code']] = entry['name']['de']

        return [vals[str(code)[:2]] if str(code)[:2] in vals else None for code in noga_codes]

    if level == 'gruppe':
        # Retrieves the group (Gruppe) names for the given NOGA codes
        noga_gruppe = requests.get(config['NOGA_GRUPPE_LINK']).json()

        # Map each NOGA code to its corresponding name
        vals = {}
        for entry in noga_gruppe['concept']['codeListEntries']:
            vals[entry['code']] = entry['name']['de']
        return [vals[str(code)[:3]] if str(code)[:3] in vals else None for code in noga_codes]

    if level == 'klasse':
        # Retrieves the class (Klasse) names for the given NOGA codes
        noga_gruppe = requests.get(config['NOGA_KLASSE_LINK']).json()

        # Map each NOGA code to its corresponding name
        vals = {}
        for entry in noga_gruppe['concept']['codeListEntries']:
            vals[entry['code']] = entry['name']['de']
        return [vals[str(code)[:4]] if str(code)[:4] in vals else None for code in noga_codes]

    if level == 'art':
        # Retrieves the type (Art) names for the given NOGA codes
        noga_gruppe = requests.get(config['NOGA_ART_LINK']).json()

        # Map each NOGA code to its corresponding name
        vals = {}
        for entry in noga_gruppe['concept']['codeListEntries']:
            vals[entry['code']] = entry['name']['de']
        return [vals[str(code)] if str(code) in vals else None for code in noga_codes]

    # Raise exception if level did not match any of the cases
    raise Exception('No matching level. Valid levels are ["abschnitt_code", "abschnitt", "abteilung", "gruppe", "klasse", "art"]')


def correct_addresses(df, df_cleans, cols):
    '''
    Apply manual address corrections based on a corrections DataFrame.

    Parameters:
    - df: DataFrame containing original company data.
    - df_cleans: DataFrame with corrected address data.
    - cols: List of columns in `df` to be updated with corrections from `df_cleans`.

    Returns:
    - df: DataFrame with addresses corrected.
    '''
     
    # For each row, replace the address information of the original df with the values in the correction df
    for _, row in df_cleans.iterrows():
        for col in cols:
            df.loc[df['uid'] == row['uid'], col] = row[col]

    return df


def transform_to_inventory_db_structure(df, legal_form_mappings, district_mappings):
    '''
    Transform the DataFrame into the format required by the posgresql table, including data normalization, generation of additional columns and selection of specific columns.

    Attention: Some settings of this function are set in the config file

    Parameters:
    - df: DataFrame containing the original data.
    - legal_form_mappings: A dictionary mapping the code of a legalForm to its label
    - district_mappings: A dictionary maping the legalSeatId to the district_id and district name

    Returns:
    - df: Transformed dataframe
    '''    

    # Get bezirk from legalseatid
    df['firmensitz_bezirk_nr'] = df['legalSeatId'].astype(str).apply(lambda x: district_mappings[x]['bezirk_id'] if x in district_mappings else None)
    df['firmensitz_bezirk'] = df['legalSeatId'].astype(str).apply(lambda x: district_mappings[x]['bezirk_name'] if x in district_mappings else None)

    # Get zusatz from address dict column
    df['zusatz'] = df['address'].apply(lambda x: x['careOf'])

    # Get legal Form code from dict column
    df['rechtsform_code'] = df['legalForm'].apply(lambda x: x['uid'])

    # Get legal form label from mapping
    df['rechtsform'] = df['rechtsform_code'].apply(lambda x: legal_form_mappings[x]['de'])

    # Download noga_codes
    df = download_noga_codes(df, 'uid', 'noga_codes')

    # Exctract noga labels from noga code
    df['noga_abschnitt_code'] = get_noga_information_from_code_by_level(df['noga_codes'], 'abschnitt_code')
    df['noga_abschnitt'] = get_noga_information_from_code_by_level(df['noga_codes'], 'abschnitt')
    df['noga_abteilung'] = get_noga_information_from_code_by_level(df['noga_codes'], 'abteilung')
    df['noga_gruppe'] = get_noga_information_from_code_by_level(df['noga_codes'], 'gruppe')
    df['noga_klasse'] = get_noga_information_from_code_by_level(df['noga_codes'], 'klasse')
    df['noga_art'] = get_noga_information_from_code_by_level(df['noga_codes'], 'art')

    # Get Zefix url from dict column
    df['zefix_web_eintrag'] = df['zefixDetailWeb'].apply(lambda x: x['de'])
    # Create bl.register link from uid
    df['registry_link'] = config['REGISTRY_REGISTRY_BASE_LINK'] + df['uid'].apply(lambda x: re.sub(r'CHE(\d{3})(\d{3})(\d{3})', r'CHE-\1.\2.\3', x))
    

    # Create wgs84 coordinates for usage in ODS
    df['koordinaten'] = [f'{lv95_wg84_transformer.transform(coord[0], coord[1])[0]}, {lv95_wg84_transformer.transform(coord[0], coord[1])[1]}' if coord[0] is not None and coord[1] is not None else None for coord in zip(df['e_eingangskoordinate_loc'], df['n_eingangskoordinate_loc'])]
   

    # Label Cleansing
    df['ort'] = df['ort'].str.replace('ohne Domizil-sans domicile', 'ohne Domizil')
    df['legalSeatId'] = df['legalSeatId'].astype(str)
    df['comment'] = df['comment'].astype(str)

    # Add update timestamp
    df['last_update'] = datetime.now()


    # Select relevant columns
    cols_select = config['COMPANY_INVENTORY_RELEVANT_COLUMNS']

    df = df[cols_select]

    return df


#################################################
####### Helper Functions for sogc messages ######
#################################################

def process_relocations(df, munic_mappings, cant_mappings, legal_form_mappings, canton_name_shorts):
    '''
    Expects a dataframe containing sogc messages and generates a df with the focus on relocations from it. This function
    does a lot of regex retrieval from the messages free text field. The results are therefore very specific to our case
    in Basel-Landschaft

    Attention: Some settings of this function are set in the config file

    Parameters:
    - df: Dataframe with sogc messages
    - munic_mappings: a dictionary with a mapping from municipality name to bfs number
    - cant_mappings: a dictionary with a mapping from a municipalities bfs number to the canton name
    - legal_form_mappings: A dictionary mapping the code of a legalForm to its label
    - canton_name_shorts: A dictionary mapping the full name of a canton to its abbreviated form

    Returns:
    - df: Filtered Dataframe for relocations with additional columns
    '''

    # Filter df to only contain rows where mutationTypes contains 'addressaenderung'
    values = ['adressaenderung']
    df = df[df['mutationTypes'].apply(contains_key, values=values)].reset_index(drop=True)

    # Remove html tags from free text field
    df['message_cleansed'] = df['message'].apply(remove_html_tags)
    
    
    df = df[~df['message_cleansed'].str.contains('infolge Verlegung des Sitzes nach')]

    ##############
    ## Set Flags for different types of relocations
    ##############

    # Set a flag for all rows where there was a relocation inside the canton. This is done by searching for a combination of keywords in the text
    df['umzug_innerhalb_kanton'] = df['message_cleansed'].str.contains('Domizil neu:', case=False) & df['message_cleansed'].str.contains(', in ', case=False) & df['message_cleansed'].str.contains('Sitz neu:', case=False)
    
    # Set a flag for all rows where there was a relocation between cantons. We therefore also need to handle cases in french and italian.
    # This is done by searching for a combination of keywords in the text
    df['zuzug_wegzug'] = df['message_cleansed'].str.contains('Domizil neu:', case=False) & df['message_cleansed'].str.contains(', bisher in ', case=False) & df['message_cleansed'].str.contains('Sitz neu:', case=False)
    df.loc[~df['zuzug_wegzug'], 'zuzug_wegzug'] = df['message_cleansed'].str.contains('Nouvelle adresse:', case=False) & df['message_cleansed'].str.contains('précédemment à', case=False) & df['message_cleansed'].str.contains('Nouveau siège:', case=False)
    df.loc[~df['zuzug_wegzug'], 'zuzug_wegzug'] = df['message_cleansed'].str.contains('Nuova sede:', case=False) & df['message_cleansed'].str.contains('finora in', case=False) & df['message_cleansed'].str.contains('Nuovo recapito:', case=False)

    # Set a flag for relocations between cantons with a special keyword (special case)
    df['zuzug_wegzug_spez'] = False
    df.loc[(df['message_cleansed'].str.contains('Sitzverlegung nach:', case=False)) | (df['message_cleansed'].str.contains('Siège transféré à', case=False)), 'zuzug_wegzug_spez'] = True

    # Set a flag for relocations inside a municipality. This is done by searching for a combination of keywords in the text
    df['umzug_innerhalb_gemeinde'] = df['message_cleansed'].str.contains('Domizil neu:', case=False) & df['message_cleansed'].str.contains(', in ', case=False) & ~df['umzug_innerhalb_kanton'] & ~df['zuzug_wegzug'] & ~df['zuzug_wegzug_spez']

    ##############
    ## Extract municipality names
    ##############

    # Some municipalities are mentioned between the substring ', in' and the next comma
    df[', in'] = df['message_cleansed'].str.extract(r'(?<=, in )(.*?)(?=,)')

    # Some municipalities are mentioned between the substring 'Sitz neu: ' and the next dot. Also for italian and french
    df['Sitz neu:'] = df['message_cleansed'].str.extract(r'Sitz neu: (.*?)(?=\. Domizil neu:)|Nuova sede: (.*?)(?=\. Nuovo recapito)|Nouveau siège: (.*?)(?=\. Nouvelle adresse)').bfill(axis=1).iloc[:, 0]
    
    # Some municiaplities are mentioned after 'bisher in' and the next comma. Also for italian and french
    df[', bisher in'] = df['message_cleansed'].str.extract(r', (?:bisher in|précédemment à|finora in)(.*?)(?=,)')

    # Some municipalities are mentioned after 'Sitzverlegung nach: ' and the next comma
    df['sitz_neu_spez'] = df['message_cleansed'].str.extract(r'(?<=Sitzverlegung nach: )(.*?)(?=\.)|(?<=Siège transféré à )(.*?)(?=\.| Nouvelle)').bfill(axis=1).iloc[:, 0]
    
    ##############
    ## Compile source and target municipalities for different types of relocations into two columns
    ##############

    # Set source and target column to None
    df['firmensitz_neu'] = None
    df['firmensitz_bisher'] = None

    # Set source and target municipalities for relocations inside a canton
    df.loc[df['umzug_innerhalb_kanton'], 'firmensitz_neu'] = df['Sitz neu:']
    df.loc[df['umzug_innerhalb_kanton'], 'firmensitz_bisher'] = df[', in']

    # Set source and target municipalities for relocations between cantons
    df.loc[df['zuzug_wegzug'], 'firmensitz_neu'] = df['Sitz neu:']
    df.loc[df['zuzug_wegzug'], 'firmensitz_bisher'] = df[', bisher in']

    # Set source and target municipalities for relocations inside municipalities
    df.loc[df['umzug_innerhalb_gemeinde'], 'firmensitz_neu'] = df[', in']
    df.loc[df['umzug_innerhalb_gemeinde'], 'firmensitz_bisher'] = df[', in']  

    # Special case for relocations between cantons
    df.loc[df['zuzug_wegzug_spez'], 'firmensitz_neu'] = df['sitz_neu_spez']
    df.loc[df['zuzug_wegzug_spez'], 'firmensitz_bisher'] = df[', bisher in']

    ##############
    ## Enrich DataFrame
    ##############

    # Cleansing
    df['firmensitz_neu'] = df['firmensitz_neu'].str.strip()
    df['firmensitz_bisher'] = df['firmensitz_bisher'].str.strip()
    df.replace(np.nan, None, inplace=True)
    
    # Get bfs code and canton for source and target municipalities
    df['firmensitz_neu_code'] = df.apply(lambda row: map_municipality_name_to_code(row['firmensitz_neu'], row['publication_date'], munic_mappings), axis=1)
    df['firmensitz_bisher_code'] = df.apply(lambda row: map_municipality_name_to_code(row['firmensitz_bisher'], row['publication_date'], munic_mappings), axis=1)
    df['firmensitz_neu_canton'] = df['firmensitz_neu_code'].apply(lambda x: get_cant_mapping(x, cant_mappings))
    df['firmensitz_bisher_canton'] = df['firmensitz_bisher_code'].apply(lambda x: get_cant_mapping(x, cant_mappings))

    # Filter messages that have nothing to do with the canton in question
    df = df[(df['firmensitz_neu_canton'] == config['CANTON']) | (df['firmensitz_bisher_canton'] == config['CANTON']) | (df['registryOfCommerceCanton'] == config['CANTON_ABBREVIATION'])]

    # Filter out messages where no successfull matching to the municipality happened
    df = df[(~df['firmensitz_neu_code'].isnull()) & (~df['firmensitz_bisher_code'].isnull())]

    # Add legalForm information
    df['legalForm_code'] = df['legalForm'].apply(lambda x: x['uid'] if 'uid' in x else None)
    df['legalForm_name'] = df['legalForm_code'].apply(lambda x: legal_form_mappings[x]['de'] if x in legal_form_mappings else None)
    
    # Add noga information
    df = download_noga_codes(df, 'uid', 'noga_code')
    df['noga_art'] = get_noga_information_from_code_by_level(df['noga_code'], 'art')
    df['noga_abschnitt_code'] = get_noga_information_from_code_by_level(df['noga_code'], 'abschnitt_code')
    df['noga_abschnitt'] = get_noga_information_from_code_by_level(df['noga_code'], 'abschnitt')
    df['noga_abteilung'] = get_noga_information_from_code_by_level(df['noga_code'], 'abteilung')

    # Add short version of canton name to df
    df['firmensitz_neu_canton_short'] = df['firmensitz_neu_canton'].apply(lambda x: canton_name_shorts[x] if x in canton_name_shorts else None)
    df['firmensitz_bisher_canton_short'] = df['firmensitz_bisher_canton'].apply(lambda x: canton_name_shorts[x] if x in canton_name_shorts else None)

    # Cattegorize type of relocation
    df['category'] = None
    df.loc[(df['firmensitz_neu_canton_short'] == config['CANTON_ABBREVIATION']) & (df['firmensitz_bisher_canton_short'] == config['CANTON_ABBREVIATION']) & (df['firmensitz_neu_code'] == df['firmensitz_bisher_code']), 'category'] = 'Domiziländerung (intrakommunal)'
    df.loc[(df['firmensitz_neu_canton_short'] == config['CANTON_ABBREVIATION']) & (df['firmensitz_bisher_canton_short'] == config['CANTON_ABBREVIATION']) & (df['firmensitz_neu_code'] != df['firmensitz_bisher_code']), 'category'] = 'Sitzverlegung intrakantonal'
    df.loc[(df['firmensitz_neu_canton_short'] == config['CANTON_ABBREVIATION']) & (df['firmensitz_bisher_canton_short'] != config['CANTON_ABBREVIATION']), 'category'] = 'Sitzverlegung Zuzug (interkantonal)'
    df.loc[(df['firmensitz_neu_canton_short'] != config['CANTON_ABBREVIATION']) & (df['firmensitz_bisher_canton_short'] == config['CANTON_ABBREVIATION']), 'category'] = 'Sitzverlegung Wegzug (interkantonal)'
    
    # Select relevant columns and rename them
    selec_cols = config['COMPANY_RELOCATIONS_DB_COLUMNS']
    col_names = config['COMPANY_RELOCATIONS_DB_COLUMN_NAMES']
    
    df = df[selec_cols]
    df.columns = col_names
    
    return df


def process_mutations(df, munic_mappings, legal_form_mappings):
    '''
    Expects a dataframe containing sogc messages and generates a df with the focus on mutations from it.

    Attention: Some settings of this function are set in the config file

    Parameters:
    - df: Dataframe with sogc messages
    - munic_mappings: a dictionary with a mapping from municipality name to bfs number
    - legal_form_mappings: A dictionary mapping the code of a legalForm to its label

    Returns:
    - df: Filtered Dataframe for relocations with additional columns
    '''

    # Filter out entries not relevant for the canton
    df = df[df['registryOfCommerceCanton'] == config['CANTON_ABBREVIATION']]


    # Filter out mutationTypes not relevant to the use case
    values = ['status.neu', 'status.aufl', 'status.aufl.liq', 'status.aufl.konk', 'status.wiederrufliq', 'status.loeschung', 'status.wiedereintrag', 'fusion', 'spaltung']
    df = df[df['mutationTypes'].apply(contains_key, values=values)]

    if len(df) == 0:
        return pd.DataFrame()
    
    # Cleansing and enriching
    df['mutationType_pretty'] = df['mutationTypes'].apply(prettify_mutation_type)
    df['message_cleansed'] = df['message'].apply(remove_html_tags)
    df['gemeinde'] = df['message_cleansed'].str.extract(r'(?<=, in )(.*?)(?=,)')
    df['gemeinde'] = df['gemeinde'].str.replace(f' {config["CANTON_ABBREVIATION"]}', f' ({config["CANTON_ABBREVIATION"]})') # Harmonizing canton abbreviation, eg Aesch BL -> Aesch (BL)
    df['gemeinde_code'] = df.apply(lambda row: map_municipality_name_to_code(row['gemeinde'], row['publication_date'], munic_mappings), axis=1)
    df['legalForm_code'] = df['legalForm'].apply(lambda x: x['uid'] if 'uid' in x else None)
    df['legalForm_name'] = df['legalForm_code'].apply(lambda x: legal_form_mappings[x]['de'] if x in legal_form_mappings else None)

    # This is a hack. We remove municipality names of entries wrongfully attributed to municipalities outside the canton
    df['gemeinde_code'] = df['gemeinde_code'].fillna(-1).astype(int)

    max_id = int(config['CANTON_MAX_MUNIC_BFS_ID'])
    min_id = int(config['CANTON_MIN_MUNIC_BFS_ID'])
    
    df.loc[(df['gemeinde_code'].astype(int) < min_id) | (df['gemeinde_code'].astype(int) > max_id), 'gmeinede'] = None
    df.loc[(df['gemeinde_code'].astype(int) < min_id) | (df['gemeinde_code'].astype(int) > max_id), 'gemeinde_code'] = None

    # Add noga information
    df = download_noga_codes(df, 'uid', 'noga_code')
    df['noga_art'] = get_noga_information_from_code_by_level(df['noga_code'], 'art')
    df['noga_abschnitt_code'] = get_noga_information_from_code_by_level(df['noga_code'], 'abschnitt_code')
    df['noga_abschnitt'] = get_noga_information_from_code_by_level(df['noga_code'], 'abschnitt')
    df['noga_abteilung'] = get_noga_information_from_code_by_level(df['noga_code'], 'abteilung')

    # Remove municipality names that cannot be found in the code lookup table and therefore are suspected to be erroneous
    df.loc[df['gemeinde_code'].isna(), 'gemeinde'] = None 

    # Select and rename columns
    selec_cols = config['COMPANY_MUTATIONS_DB_COLUMNS']
    col_names = config['COMPANY_MUTATIONS_DB_COLUMN_NAMES']

    df = df[selec_cols]
    df.columns = col_names

    return df

def contains_key(entries, values):
    '''
    Checks if certain values are in a dictionary object. Used for mutation types of sogc messages
    '''
    for entry in entries:
        if entry['key'] in values:
            return True
    return False


def remove_html_tags(text):
    '''
    Removes html tags from a string
    '''
    html_pattern = re.compile('<.*?>')
    text = re.sub(html_pattern, '', text).replace('&amp;', '&').replace('&quot;', '"').replace('&apos;', "'")

    return text
    
    
def prettify_mutation_type(mutation_types):
    '''
    Returns simplified mutation types
    '''

    # Mapping object
    mapping = { 1: 'Statusänderung (alle)', 2: 'Neueintragung', 3: 'Auflösung (alle)', 4: 'Auflösung', 5: 'Auflösung infolge Konkurs', \
        6: 'Widerruf der Auflösung', 7: 'Löschung', 8: 'Wiedereintragung',  9: 'Änderung Firma (Name)', 10: 'Änderung Rechtsform', 11: 'Änderung Zweck', \
        12: 'Kapitaländerung (alle)', 13: 'Änderung nominelles Kapital', 14: 'Änderung liberiertes Kapital', 15: 'Änderung Kapitalstückelung', 16: 'Adressänderung', \
        17: 'Änderung Organe / Vertretung', 18: 'Änderung UID', 19: 'Fusion', 20: 'Spaltung', 21: 'Vermögensübertragung', 22: 'Änderung Kapitalband'
    }

    # Turn mutation_types into list of keys
    keys = [val['id'] for val in mutation_types if val['id']] 

    # In our ogd table there are some mutation types that supersede others. The following case list must be in that order
    # so that the simplified mutation type is correct. As a result, some mutation types in the mapping that always occur
    # together with one of the prioritized types vanish in the simplified version.
    if 6 in keys and 7 in keys:
        return mapping[7]
    elif 6 in keys and 8 in keys:
        return mapping[8]
    elif 7 in keys and 19 in keys:
        return 'Löschung infolge Fusion'
    elif 2 in keys:
        return mapping[2]
    elif 4 in keys:
        return mapping[4]
    elif 5 in keys:
        return mapping[5]
    elif 6 in keys:
        return mapping[6]
    elif 7 in keys:
        return mapping[7]
    elif 8 in keys:
        return mapping[8]
    elif 19 in keys:
        return mapping[19]
    elif 20 in keys:
        return mapping[20]
    else:
        return mapping[keys[0]]


def map_municipality_name_to_code(municipality, date, mapping):
    '''
    Expects the name of a municipality and returns the municipality's historized code
    '''
    # Abbord if municipality name is None
    if pd.isna(municipality):
        return None
    
    # Return newest code if municipality name is in mapping
    elif municipality.lower() in mapping:
        codes = [val for val in mapping[municipality.lower()]]
        codes = sorted(codes, key=lambda x: x[0], reverse=True)
        return codes[0][1]
    
    # If the municipality name is not in the mapping, try again with a different schema where the canton abbreviation is not in parantheses
    elif municipality.replace('(', '').replace(')', '').lower() in [key.replace('(', '').replace(')', '').lower() for key in mapping.keys()]:
        key = [key for key in mapping.keys() if municipality.replace('(', '').replace(')', '').lower() == key.replace('(', '').replace(')', '').lower()][0]
        codes = [val for val in mapping[key]]
        codes = sorted(codes, key=lambda x: x[0], reverse=True)
        return codes[0][1]
    
    # Return None if no match was found
    else:
        return None

def get_cant_mapping(cant, cant_mappings):
    '''
    Expects an abbreviated canton name (e.g. Basel-Landschaft) and returns its abbreviated version (e.g. BL)
    '''
    if cant not in cant_mappings:
        return None
    else:
        return cant_mappings[cant]



def get_default_proxies():
    '''
    Returns proxy information from environment variables. A dictionary with the http and https url
    '''
    return {
        'http': os.getenv("https_proxy"),
        'https' : os.getenv("https_proxy")
        }

def download_municipality_mappings():
    '''
    Returns a dictionary with the name of a municipality as key and the historized bfs number as value

    Attention: Some settings of this function are set in the config file
    '''

    # Download data
    url = config['BFS_COMMUNES_SNAPSHOT']
    response = requests.get(url)
    df_munic = pd.read_csv(StringIO(response.content.decode('utf-8')))

    # Cleansing
    df_munic = df_munic.astype(str)
    df_munic = df_munic[df_munic['Level'] == '3'].reset_index(drop=True) # Only use municipalities (2 would be districts, 1 would be cantons)
    df_munic['ValidFrom'] = pd.to_datetime(df_munic['ValidFrom'], format='%d.%m.%Y')

    df_munic = df_munic.sort_values(by='ValidFrom', ascending=False).reset_index(drop=True)

    # Create dictionary
    munic_mappings = {}
    for _, row in df_munic.iterrows():
        if row['Name_de'].lower() not in munic_mappings:
            munic_mappings[row['Name_de'].lower()] = [(row['ValidFrom'], row['Identifier'])]
        else:
            munic_mappings[row['Name_de'].lower()] += [(row['ValidFrom'], row['Identifier'])]

    return munic_mappings

def download_legal_form_mappings():
    '''
    Returns a dictionary with the code of a legalForm as key and the label of the legalForm as value

    Attention: Some settings of this function are set in the config file
    '''
    legal_forms =  requests.get(config['LEGAL_FORMS']).json()
    legal_form_mappings = {}
    for entry in legal_forms['concept']['codeListEntries']:
        legal_form_mappings[entry['code']] = {'de': entry['name']['de'], 'en': entry['name']['en'], 'fr': entry['name']['fr'], 'it': entry['name']['it']}

    return legal_form_mappings


def download_canton_code_mappings():
    '''
    Returns a dictionary with the code of a canton as key and the full name of the canton as value

    Attention: Some settings of this function are set in the config file
    '''
    # Download
    canton_inf =  requests.get(config['CANTON_INFORMATION']).json()

    # Create dictionary
    canton_inf_mappings = {}
    for entry in canton_inf['concept']['codeListEntries']:
        for key in entry['name'].keys():
            canton_inf_mappings[str(entry['name'][key])] = entry['annotations'][1]['title']

    return canton_inf_mappings


def download_canton_mappings():
    '''
    Returns a dictionary with the identifier of a commune as key and its canton as value

    Attention: Some settings of this function are set in the config file
    '''
    # Download
    url = config['BFS_COMMUNE_LEVELS']
    response = requests.get(url)
    df_cant = pd.read_csv(StringIO(response.content.decode('utf-8')), low_memory=False)

    # Cleansing
    df_cant = df_cant.astype(str)

    # Create dictionary
    cant_mappings = {}
    for _, row in df_cant.iterrows():
        # The cantons are sometimes listed in two languages (e.g. Bern / Berne) we always use the first label which is the german one
        cant_mappings[row['CODE_OFS']] = row['HR_HGDE_HIST_L1_Name_de'].split('/')[0].strip()

    return cant_mappings


#################################################
#### Database Interaction Helper Functions ######
#################################################

def replace_inventory_database(df):
    '''
    This function truncates the inventory table and replaces it with the content of the df
    '''
    df.columns = df.columns.str.lower()
    with engine.connect() as connection, connection.begin():

        schema = config['DB_SCHEMA']
        table = config['COMPANY_INVENTORY_DB_TABLE']

        # Truncate the table first
        truncate_query = f'TRUNCATE TABLE {schema}.{table}'
        connection.execute(text(truncate_query))

        # Upload new data
        df.to_sql(table, connection, schema=schema, if_exists='append', index=False)


def update_inventory_database(updated_uids, df):
    '''
    This function removes the companies with the provided uids from the table and adds the rows from the df

    Attention: Some settings of this function are set in the config file

    Parameters:
    - updated_uids: uids to remove
    - df: df to add to the table

    Returns:
    - df_deleted: dataframe with deleted rows
    - df: dataframe with added rows
    '''

    df.columns = df.columns.str.lower()

    with engine.connect() as connection, connection.begin():

        schema = config['DB_SCHEMA']
        table = config['COMPANY_INVENTORY_DB_TABLE']

        # Convert the list of UIDs into a string of comma-separated values for the SQL query
        uids_str = ','.join(f"'{uid}'" for uid in updated_uids)

        # Get all companies that are going to be removed in the next step
        df_deleted = pd.read_sql(text(f'SELECT * FROM {schema}.{table} WHERE uid IN ({uids_str})'), connection)

        # Delete rows with UIDs present in the updated_uids list
        connection.execute(text(f'DELETE FROM {schema}.{table} WHERE uid IN ({uids_str})'))

        # Upload the DataFrame with new entries
        if len(df) > 0:
            df.to_sql(table, connection, schema=schema, if_exists='append', index=False)

        return df_deleted, df
    

def update_mutation_database(df):
    '''
    This function updates the mutations table. It removes all entries with shab_ids in the df and adds the rows of the df

    Attention: Some settings of this function are set in the config file
    '''
    if len(df) > 0:
        shab_ids_to_remove = ', '.join([f"'{id}'" for id in df['id_shab']])
        with engine.connect() as connection, connection.begin():
            schema = config['DB_SCHEMA']
            table = config['COMPANY_MUTATIONS_DB_TABLE']

            connection.execute(text(f'DELETE FROM {schema}.{table} WHERE id_shab IN ({shab_ids_to_remove})'))
            df.to_sql(table, connection, schema=schema, if_exists='append', index=False)


def update_relocation_database(df):
    '''
    This function updates the relocation table. It removes all entries with shab_ids in the df and adds the rows of the df

    Attention: Some settings of this function are set in the config file
    '''
    if len(df) > 0:
        shab_ids_to_remove = ', '.join([f"'{id}'" for id in df['id_shab']])
        with engine.connect() as connection, connection.begin():

            schema = config['DB_SCHEMA']
            table = config['COMPANY_RELOCATIONS_DB_TABLE']
            
            connection.execute(text(f'DELETE FROM {schema}.{table} WHERE id_shab IN ({shab_ids_to_remove})'))
            
            if len(df) > 0:
                df.to_sql(table, connection, schema=schema, if_exists='append', index=False)


def get_inventory_ogd_table():
    '''
    This returns the inventory ogd table for ods

    Attention: Some settings of this function are set in the config file

    Returns:
    - df: Dataframe with all columns as strings
    '''
    with engine.connect() as connection, connection.begin():

        columns = config['COMPANY_INVENTORY_OGD_COLUMNS']
        new_names = config['COMPANY_INVENTORY_OGD_COLUMN_NAMES']
        schema = config['DB_SCHEMA']
        table = config['COMPANY_INVENTORY_DB_TABLE']

        df = pd.read_sql(text(f'SELECT {", ".join(columns)} FROM {schema}.{table}'), connection, dtype=str)

        df.columns = new_names

        for col in df.columns:
            df[col] = df[col].astype(str)

        df.replace('None', None, inplace=True)

        return df
    
def get_mutations_ogd_table():
    '''
    This returns the mutations ogd table for ods

    Attention: Some settings of this function are set in the config file

    Returns:
    - df: Dataframe with all columns as strings
    '''

    with engine.connect() as connection, connection.begin():

        columns = config['COMPANY_MUTATIONS_DB_COLUMN_NAMES']
        schema = config['DB_SCHEMA']
        table = config['COMPANY_MUTATIONS_DB_TABLE']

        df = pd.read_sql(text(f'SELECT {", ".join(columns)} FROM {schema}.{table} ORDER BY id_shab DESC'), connection, dtype=str)
        
        for col in df.columns:
            df[col] = df[col].astype(str)

        df.replace('None', None, inplace=True)

        return df
    
def get_relocations_ogd_table():
    '''
    This returns the relocation ogd table for ods

    Attention: Some settings of this function are set in the config file

    Returns:
    - df: Dataframe with all columns as strings
    '''
    with engine.connect() as connection, connection.begin():

        columns = config['COMPANY_RELOCATIONS_DB_COLUMN_NAMES']
        schema = config['DB_SCHEMA']
        table = config['COMPANY_RELOCATIONS_DB_TABLE']

        df = pd.read_sql(text(f'SELECT {", ".join(columns)} FROM {schema}.{table} ORDER BY id_shab DESC'), connection, dtype=str)
        
        for col in df.columns:
            df[col] = df[col].astype(str)

        df.replace('None', None, inplace=True)

        return df
    

##############################################
########## Helpber Functions for Digest ######
##############################################

def address_corrections_digest(df):
    '''
    This method tests if all cleansed addresses from the db were used in the generation of the inventory table
    and returns a String that lists all unused uids in the address correction table if there are any.

    Parameters:
    - Pandas df: A pandas dataframe with the finished inventory table

    Returns:
    - String: Description of deprecated addresses in the database
    '''
    df = df[df['lokalisierungsmethode'] == 'kgwr match with replaced address']
    df_cleansed = download_cleansed_addresses()
    df_cleansed = df_cleansed[~df_cleansed['uid'].isin(df['uid'])]

    if len(df_cleansed) == 0:
        return '-'
    else:
        return f'Adresskorrekturen veraltet für folgende Firmen: {", ".join(df_cleansed["uid"])}'
