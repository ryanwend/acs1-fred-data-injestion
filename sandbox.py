
#%%
from modules import functions as fcs
import pandas as pd
import os

# Read Config file 
config = fcs.read_config()

# Get ACS Data YoY
start_year = 2010
end_year = 2022
state_code = '08'  # CO state code
vars = config['acs_variables_to_grab']

df_acs = fcs.fetch_acs_data_parallelized(start_year, end_year, state_code, vars)

# Extract data from 'response_summary' column and expand the result into new columns
df_acs[['County', 'State', 'CountyFIPS', 'StateFIPS', 'MapFIPS']] = df_acs['index'].astype(str).apply(fcs.extract_info).apply(pd.Series)
df_acs.drop(columns=['index'], inplace=True)
df_acs





#%% Get Annual Mortgage Rates from FRED
api_key = os.environ.get('fred_api_key')
start_year = 2010
end_year = 2022

df_annual_mortgage_rates = fcs.get_annual_mortgage_rates(api_key, start_year, end_year)
df_final = pd.merge(df_acs, df_annual_mortgage_rates, on='Year', how='left')

# Add the MonthlyMortgagePayment column
fcs.add_monthly_payment_column(df_final, 'MedianHomeValue', 'MortgageRate', 3.5, 30)


# Final Additions
df_final['AnnualRentPayment_Calculated'] = df_final['MedianMonthlyRent'] * 12
df_final['RentPercentOfIncome_Calculated'] = df_final['AnnualRentPayment_Calculated'] / df_final['MedianAnnualHouseholdIncome']
df_final['MortgagePercentOfIncome_Calculated'] = df_final['AnnualMortgagePayment_Calculated'] / df_final['MedianAnnualHouseholdIncome']

df_final['RentPercentOfIncomeCurrentRenters_Calculated'] = df_final['AnnualRentPayment_Calculated'] / df_final['MedianIncome(renters)']
df_final['MortgagePercentOfIncomeCurrentHomeowners_Calculated'] = df_final['AnnualMortgagePayment_Calculated'] / df_final['MedianIncome(home-owners)']

#%%
df_final
# %%
