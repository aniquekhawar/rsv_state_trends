import pandas as pd
import numpy as np
from functools import reduce

state_codes = ['AL','AK','AZ','AR','CA','CO','CT','DE','DC','FL','GA',
              'HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA',
              'MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY',
              'NC','ND','OH','OK','OR','PA', 'PR','RI','SC','SD','TN','TX',
              'UT','VT','VA','WA','WV','WI','WY']

antigen_url = 'https://www.cdc.gov/surveillance/nrevss/images/rsvstate/RSV1PPCent3AVG_State'
pcr_url2 = 'https://www.cdc.gov/surveillance/nrevss/images/rsvstate/RSV4PPCent3AVG_State'

antigen_url_list = [f'{antigen_url}{state}.htm' for state in state_codes]
pcr_url_list = [f'{pcr_url2}{state}.htm' for state in state_codes]

def clean_data(df):
    df['RepWeekDate'] = pd.to_datetime(df['RepWeekDate'])
    # check the last column
    last_col_name = df.columns.tolist()[-1]
    df['Percent Positive'] /= 100.0
    if 'PCR' in last_col_name:
        df['Positive_PCR_Tests'] = df['Percent Positive'] * df['Total PCR Tests']
        test_type = 'PCR'
    elif 'Antigen' in last_col_name:
        df['Positive_Antigen_Tests'] = df['Percent Positive'] * df['Total Antigen Detection Tests']
        test_type = 'Antigen'
    df.drop(['Unnamed: 0'], axis = 1, inplace = True)
    rename_dict = {'RepWeekDate': 'Date', 'StateID': 'State', 'Percent Positive': f'%_Positive_{test_type}_Tests',
                    'Total Antigen Detection Tests': 'Total_Antigen_Detection_Tests', 'Total PCR Tests': 'Total_PCR_Tests'}
    df.rename(columns = rename_dict, inplace = True)
    return df
    
def get_data(url_list):
    frames = []
    for url in url_list:
        try:
            temp_df = pd.read_html(url)[0]
            frames.append(temp_df)
        except:
            print(f'Unable to read from URL: {url}')
    df = pd.concat(frames, ignore_index = True)
    df = clean_data(df)
    return df

antigen_data = get_data(antigen_url_list)
pcr_data = get_data(pcr_url_list)

merged = antigen_data.merge(pcr_data, on = ['Date', 'State'], how = 'outer', validate = '1:1')
# error handling
merged = merged.replace(np.inf, np.nan)

merged.to_csv('rsv_dataset.csv', index = False)