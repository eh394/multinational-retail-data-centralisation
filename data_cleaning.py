import yaml
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import inspect
import numpy as np
import pandas as pd
pd.options.display.max_columns = None
pd.options.display.max_rows = None
# from data_extraction import DataExtractor
# from database_utils import DatabaseConnector

class DataClean:
    
    def clean_user_data(self, df):
        
        users = df.copy()

        # Denote NULL entries as format recognizable by pandas / numpy
        users = users.replace('NULL', np.nan)

        # Clean columns with date of birth and join date
        date_fix = lambda x: pd.to_datetime(x, format = '%Y-%m-%d' , errors = 'coerce')
        users['date_of_birth'] = users['date_of_birth'].apply(date_fix)
        users['join_date'] = users['join_date'].apply(date_fix)
        # users['date_of_birth'] = users['date_of_birth'].apply(lambda x: pd.to_datetime(x, format = '%Y-%m-%d' , errors = 'coerce'))
        # users['join_date'] = users['join_date'].apply(lambda x: pd.to_datetime(x, format = '%Y-%m-%d' , errors = 'coerce'))

        # Clean columns with country and country code information
        countries = ['United Kingdom', 'United States', 'Germany']
        country_codes = ['GB', 'US', 'DE']
        users['country'] = users['country'].apply(lambda x: x if x in countries else np.nan)
        users['country_code'] = users['country_code'].apply(lambda x: x if x in country_codes else ('GB' if x == 'GGB' else np.nan))

        # Clean column with phone number
        users['phone_number'] = users['phone_number'].replace('[\D]', '', regex=True).str[-10:].str.lstrip('0')

        # Dropping nan values and resetting index
        users = users.drop('index', axis = 1)
        users = users.dropna() # thresh = 11
        users = users.dropna(subset = ['user_uuid'])
        users = users.reset_index(drop=True)

        return users

    def clean_card_data(self, df):

        cards = df.copy()

        # Check card providers are valid and corresponding number of digits in the card number is correct
        providers = {'VIS digit': [13], 'VISA 16 digit': [16], 'VISA 19 digit': [19], 'JCB 15 digit': [15], 'JCB 16 digit': [16], 'Discover': [16], 'Mastercard': [16], 'Maestro': range(12, 20), 'American Express': [15], 'Diners Club / Carte Blanche': [14]}
        cards['card_provider'] = cards['card_provider'].apply(lambda x: x if x in providers else np.nan)
        cards = cards.dropna()
        cards['card_check'] = cards.apply(lambda row: True if (len(str(row['card_number'])) in providers[row['card_provider']]) else np.nan, axis = 1)

        # Clean columns with expiry date and confirmed date of payment
        cards['date_payment_confirmed'] = cards['date_payment_confirmed'].apply(lambda x: pd.to_datetime(x, format = '%Y-%m-%d' , errors = 'coerce'))
        cards['expiry_date'] = cards['expiry_date'].apply(lambda x: pd.to_datetime(x, format = '%m/%y' , errors = 'coerce'))

        # Dropping nan values, duplicates, card check column that is no longer required and resetting index
        cards = cards.dropna() # thresh = 4
        cards.drop_duplicates(inplace = True)
        cards = cards.drop('card_check', axis = 1)
        cards = cards.reset_index(drop=True)

        return cards

    
    def clean_stores_data(self, df):
        
        stores = df.copy()

        # Clean column with opening date
        stores['opening_date'] = stores['opening_date'].apply(lambda x: pd.to_datetime(x, format = '%Y-%m-%d' , errors = 'coerce'))

        # Clean up continent column
        continents = ['Europe', 'America']
        stores['continent'] = stores['continent'].apply(lambda x: x if x in continents else('Europe' if 'Europe' in str(x) else('America' if 'America' in str(x) else np.nan)))

        # Clean up country code column
        country_codes = ['GB', 'US', 'DE']
        stores['country_code'] = stores['country_code'].apply(lambda x: x if x in country_codes else np.nan)

        # Clean up store type column
        store_types = ['Local', 'Super Store', 'Mall Kiosk', 'Outlet', 'Web Portal']
        stores['store_type'] = stores['store_type'].apply(lambda x: x if x in store_types else np.nan)

        # Clean column with locality    
        stores['locality'] = stores['locality'].replace('[\d]', np.nan, regex=True)

        # Clean column with address
        stores['address'] = stores['address'].apply(lambda x: np.nan if len(str(x).split())==1 else x)

        # Clean up staff numbers column
        stores['staff_numbers'] = stores['staff_numbers'].apply(lambda x: pd.to_numeric(x, errors = 'coerce'))

        # Clean up longitude and latitide columns
        stores['longitude'] = stores['longitude'].apply(lambda x: pd.to_numeric(x, errors = 'coerce'))
        stores['latitude'] = stores['latitude'].apply(lambda x: pd.to_numeric(x, errors = 'coerce'))

        stores = stores.drop(['index', 'lat'], axis = 1)
        stores.dropna(inplace = True)
        stores.drop_duplicates(inplace = True)
        stores = stores.reset_index(drop = True)

        return stores

    
    def convert_product_weights(self, df):
        
        
        products = df.copy()

        products['weight'] = products['weight'].apply(lambda x: str(x)[: (str(x).index('g')+1)] if 'g' in str(x) else x)

        products['weight'] = products['weight'].apply(lambda x:
        pd.to_numeric(str(x)[:-2], errors = 'coerce') if str(x)[-2:] == 'kg' else
        (0.001*pd.to_numeric(str(x).strip('g').split(' ')[0]) * pd.to_numeric(str(x).strip('g').split(' ')[-1]) if ('x' in str(x) and str(x)[-1] == 'g') else 
        (0.001*pd.to_numeric(str(x)[:-1], errors = 'coerce') if str(x)[-1] == 'g' else
        (0.001*pd.to_numeric(str(x)[:-2], errors = 'coerce') if str(x)[-2:] == 'ml' else
        (0.0284*pd.to_numeric(str(x)[:-2], errors = 'coerce') if str(x)[-2:] == 'oz' else 
        np.nan)))))
        products['weight'] = products['weight'].apply(lambda x: round(x, 2))

        return products

    def clean_product_data(self, df):

        products = df 

        # tidy up category column
        categories = ['homeware', 'toys-and-games', 'food-and-drink', 'pets', 'sports-and-leisure', 'health-and-beauty', 'diy']
        products['category'] = products['category'].apply(lambda x: x if x in categories else np.nan)

        # tidy up EAN column, could add another clause to replace any values which contain letters with nan
        products['EAN'] = products['EAN'].apply(lambda x: x if len(str(x)) in [12, 13] else np.nan)

        # convert date added column entries to datetime data type
        products['date_added'] = products['date_added'].apply(lambda x: pd.to_datetime(x, format = '%Y-%m-%d' , errors = 'coerce'))

        # tidy up uuid column
        products['uuid'] = products['uuid'].apply(lambda x: x if len(str(x))==36 else np.nan)

        # tidy up price column and convert to numerical
        products['product_price'] = products['product_price'].replace('[a-zA-Z]', np.nan, regex=True)
        products['product_price'] = products['product_price'].apply(lambda x: round(pd.to_numeric(str(x).strip('£'), errors='coerce'), 2))

        # tidy up removed column; note misspelling of available
        availability = ['Still_avaliable', 'Removed']
        products['removed'] = products['removed'].apply(lambda x: x if x in availability else np.nan)

        # delete unnecessary columns, nan entries and reset index (no duplicates confirmed)
        products = products.drop('Unnamed: 0', axis=1)
        products.dropna(inplace = True)
        products.drop_duplicates(inplace=True)
        products = products.reset_index(drop=True)

        return products

    def clean_orders_data(self, df):
        
        orders = df.copy()
        
        orders = orders.drop(['first_name', 'last_name', '1', 'level_0', 'index'], axis=1)
        orders['card_number'] = orders['card_number'].replace('[a-zA-Z]', np.nan, regex=True)
        orders['product_quantity'] = orders['product_quantity'].apply(lambda x: pd.to_numeric(x, errors = 'coerce'))
        orders = orders.reset_index(drop=True)

        return orders

    def clean_date_data(self, df):
        
        dates = df.copy()

        dates.replace('NULL', np.nan, inplace=True)

        dates['timestamp'] = dates['timestamp'].apply(lambda x: pd.to_datetime(x, format= '%H:%M:%S', errors = 'coerce')).dt.time

        months = [*range(1,13)]
        dates['month'] = dates['month'].apply(lambda x: x if pd.to_numeric(x, errors='coerce') in months else np.nan)

        days = [*range(1,32)]
        dates['day'] = dates['day'].apply(lambda x: x if pd.to_numeric(x, errors='coerce') in days else np.nan)

        years = [*range(1980,2023)]
        dates['year'] = dates['year'].apply(lambda x: x if pd.to_numeric(x, errors='coerce') in years else np.nan)

        dates['date_uuid'] = dates['date_uuid'].apply(lambda x: x if len(str(x))==36 else np.nan)

        periods = ['Morning', 'Midday', 'Evening', 'Late_Hours']
        dates['time_period'] = dates['time_period'].apply(lambda x: x if x in periods else np.nan)

        dates.dropna(inplace = True)
        dates.drop_duplicates(inplace=True)
        dates = dates.reset_index(drop=True)

        return dates

# df_temp = dates[dates['time_period']=='Late_Hours']
# print(df_temp['timestamp'].min())
# print(df_temp['timestamp'].max())

# cards[['card_provider', 'card_number']] = cards[['card_provider', 'card_number']].apply(lambda i: x if x in providers else np.nan)
# cards['card_provider'] = cards['card_provider'].apply(lambda x: x if x in providers else np.nan)
# cards['card_number_length'] = cards.apply(lambda row: len(str(row['card_number'])), axis=1)


# users = DataClean().clean_user_data()
# print(users.head())

# FURTHER NOTES ON USERS CLEANING:
# Cross-check country entry against country code entry and turn into categorical data
# Check that user id is all the same length and format