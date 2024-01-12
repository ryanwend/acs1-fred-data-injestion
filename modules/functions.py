#%%
import censusdata
import pandas as pd
import re
from concurrent.futures import ThreadPoolExecutor
import requests
import numpy_financial as npf
import os
import yaml
import requests
from bs4 import BeautifulSoup

def get_acs_variable_label(variable_name, year=2022, dataset='acs1'):
    # Fetch HTML content from the Census API variables page
    url = f'https://api.census.gov/data/{year}/acs/acs1/variables.html'
    response = requests.get(url)
    
    # Check if the request was successful
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the table containing variable information
        table = soup.find('table')
        
        # Search for the variable name and extract the label
        for row in table.find_all('tr'):
            columns = row.find_all('td')
            if len(columns) >= 2 and columns[0].text.strip() == variable_name:
                return columns[1].text.strip()
        
        print(f"Variable {variable_name} not found in the table.")
        return None
    else:
        print(f"Failed to retrieve information from the Census API variables page.")
        return None

def read_config(config_path = 'config/config.yaml'):
    """
    Reads the yaml file from the config_path and converts it to a dict
    """
    read = open(config_path, 'r')
    data = read.read()
    config_data = yaml.safe_load(data)
    return config_data

def calculate_monthly_payment(home_value, down_payment_percent, interest_rate, loan_term_years):
    loan_amount = home_value * (1 - down_payment_percent / 100.0)
    monthly_interest_rate = interest_rate / 100.0 / 12.0
    total_payments = loan_term_years * 12
    monthly_payment = -npf.pmt(monthly_interest_rate, total_payments, loan_amount)
    return monthly_payment


def add_monthly_payment_column(df, home_value_column, mortgage_rate_column, down_payment_percent=3.5, loan_term_years=30):
    df['MonthlyMortgagePayment_Calculated'] = df.apply(
        lambda row: calculate_monthly_payment(row[home_value_column], down_payment_percent, row[mortgage_rate_column], loan_term_years),
        axis=1
    )
    df['DownPaymentAmount_Calculated'] = df[home_value_column] * down_payment_percent / 100.0
    df['AnnualMortgagePayment_Calculated'] = df['MonthlyMortgagePayment_Calculated'] * 12


def get_annual_mortgage_rates(api_key, start_year, end_year, series_id='MORTGAGE30US'):
    # Construct the FRED API URL
    url = f'https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={api_key}&file_type=json'

    # Fetch data from FRED API
    response = requests.get(url)
    data = response.json()

    # Extract relevant information from the response
    observations = data['observations']
    dates = [obs['date'] for obs in observations]
    values = [float(obs['value']) for obs in observations]

    # Create a DataFrame
    df_mortgage_rates = pd.DataFrame({'Date': dates, 'MortgageRate': values})

    # Convert 'Date' to datetime
    df_mortgage_rates['Date'] = pd.to_datetime(df_mortgage_rates['Date'])

    # Add 'Year' column
    df_mortgage_rates['Year'] = df_mortgage_rates['Date'].dt.year

    # Set 'Date' as the index
    df_mortgage_rates.set_index('Date', inplace=True)

    # Filter the data based on the specified start and end years
    df_mortgage_rates = df_mortgage_rates[(df_mortgage_rates['Year'] >= start_year) & (df_mortgage_rates['Year'] <= end_year)]

    # Resample the data to an annual frequency
    df_mortgage_rates_resampled = df_mortgage_rates.resample('A').mean().reset_index(drop=True)

    return df_mortgage_rates_resampled


def get_variable_info(year, variables, dataset='acs1'):
    censusdata.printtable(dataset, year)

def fetch_acs_data_parallel(year, state_code, variable_list):
    
    try:
        data_county = censusdata.download('acs1', year, censusdata.censusgeo([('state', state_code), ('county', '*')]), variable_list)
        data_state = censusdata.download('acs1', year, censusdata.censusgeo([('state', state_code)]), variable_list)

        df_county = pd.DataFrame(data_county).reset_index()
        df_state = pd.DataFrame(data_state).reset_index()
        df_year = pd.concat([df_county, df_state], ignore_index=True)
        
        df_year['Year'] = year
    except Exception as e:
        print(f"Error fetching data for {year}: {e}")
        return pd.DataFrame()

    return df_year

def fetch_acs_data_parallelized(start_year, end_year, state_code, variable_list):
    with ThreadPoolExecutor() as executor:
        dfs = list(executor.map(lambda year: fetch_acs_data_parallel(year, state_code, variable_list), range(start_year, end_year + 1)))

    result_df = pd.concat(dfs, ignore_index=True)

    return result_df



# Define a function to extract information from the string
def extract_info(row):
    match = re.match(r'(?P<County>[\w\s]+),\s(?P<State>[\w\s]+):.*state:(?P<StateFIPS>\d+)(> county:(?P<CountyFIPS>\d+))?', row)
    if match:
        return match.group('County'), match.group('State'), match.group('CountyFIPS'), match.group('StateFIPS'), match.group('StateFIPS')+match.group('CountyFIPS')
    else:
        return 'Statewide', 'Colorado', '000', '08', '08000'
