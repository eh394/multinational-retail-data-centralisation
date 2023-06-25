import yaml
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import inspect
import numpy as np
import pandas as pd
pd.options.display.max_columns = None
pd.options.display.max_rows = None
import re
# from data_extraction import DataExtractor
# from database_utils import DatabaseConnector

class DataClean:
    
    def clean_user_data(self, df):
        
        users = df.copy()

        # Denote NULL entries as format recognizable by pandas / numpy
        users = users.replace('NULL', np.nan)

        # Clean columns with first and last names
        users[['first_name', 'last_name']] = users[['first_name', 'last_name']].replace('[^a-zA-Z-]', np.nan, regex=True)
      
        # Clean columns with date of birth and join date
        users[['date_of_birth', 'join_date']] = users[['date_of_birth', 'join_date']].apply(lambda x: pd.to_datetime(x, format='%Y-%m-%d' , errors='coerce'))
        
        # Drop row with duplicated email address
        users.drop_duplicates(subset='email_address', keep='first', inplace=True)
           
        # Clean columns with country and country code information
        countries = ['United Kingdom', 'United States', 'Germany']
        country_codes = ['GB', 'US', 'DE']
        users.country = users.country.apply(lambda x: x if x in countries else np.nan)
        users.country_code = users.country_code.apply(lambda x: x if x in country_codes else ('GB' if x == 'GGB' else np.nan))

        # Clean column with phone number
        users.phone_number = users.phone_number.replace('[\D]', '', regex=True).str[-10:].str.lstrip('0')

        # Verify format of user uuid
        users.user_uuid = users.user_uuid.apply(lambda x: x if re.match('^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', str(x)) else np.nan)

        # Dropping redundant index column and resetting index
        users.drop('index', axis = 1, inplace=True)
        users.dropna(inplace=True, subset=['user_uuid'])
        users.reset_index(drop=True, inplace=True)

        return users


    def clean_card_data(self, df):

        cards = df.copy()

        # Check card providers are valid and corresponding number of digits in the card number is correct
        providers = {'VISA 13 digit': [13], 'VISA 16 digit': [16], 'VISA 19 digit': [19], 'JCB 15 digit': [15], 'JCB 16 digit': [16], 'Discover': [16], 'Mastercard': [16], 'Maestro': range(12, 21), 'American Express': [15], 'Diners Club / Carte Blanche': [14]}
        cards.card_provider = cards.card_provider.apply(lambda x: x if x in providers else np.nan)
        cards.dropna(inplace=True, subset = ['card_provider']) # without this line next line will result in key error
        cards.card_number = cards.card_number.replace('[\D]', '', regex=True)        
        cards['card_check'] = cards.apply(lambda row: True if (len(str(row['card_number'])) in providers[row['card_provider']]) else np.nan, axis = 1)
        cards.card_number = cards.card_number.apply(lambda x: str(x) if len(str(x)) in range(12,20) else np.nan)

        # Clean columns with expiry date and confirmed date of payment
        cards.date_payment_confirmed = cards.date_payment_confirmed.apply(lambda x: pd.to_datetime(x, format = '%Y-%m-%d' , errors = 'coerce')).dt.date
        cards.expiry_date = cards.expiry_date.apply(lambda x: pd.to_datetime(x, format = '%m/%y' , errors = 'coerce')).dt.date

        # Dropping nan values, duplicates, card check column that is no longer required and resetting index
        cards.dropna(inplace=True, subset=['card_number'])
        cards.drop_duplicates(inplace = True)
        cards.drop('card_check', axis = 1, inplace=True)
        cards.reset_index(drop=True, inplace=True)

        return cards

    
    def clean_stores_data(self, df):
        
        stores = df.copy()

        # Clean column with address
        stores.address = stores.address.apply(lambda x: np.nan if len(str(x).split())==1 else x)

        # Clean column with opening date
        stores.opening_date = stores.opening_date.apply(lambda x: pd.to_datetime(x, format = '%Y-%m-%d' , errors = 'coerce')).dt.date

        # Clean up continent column
        continents = ['Europe', 'America']
        stores.continent = stores.continent.apply(lambda x: x if x in continents else('Europe' if 'Europe' in str(x) else('America' if 'America' in str(x) else np.nan)))

        # Clean up country code column
        country_codes = ['GB', 'US', 'DE']
        stores.country_code = stores.country_code.apply(lambda x: x if x in country_codes else np.nan)

        # Clean up store type column
        store_types = ['Local', 'Super Store', 'Mall Kiosk', 'Outlet', 'Web Portal']
        stores.store_type = stores.store_type.apply(lambda x: x if x in store_types else np.nan)

        # Clean column with locality    
        stores.locality.replace('[\d]', np.nan, regex=True, inplace=True)

        # Check store_code against specific format
        stores.store_code = stores.store_code.apply(lambda x: x if re.match('^[A-Z]{2,3}-[A-Z0-9]{8}$', str(x)) else np.nan)

        # Clean up staff numbers, longitude and latitide columns
        stores[['staff_numbers', 'longitude', 'latitude']] = stores[['staff_numbers', 'longitude', 'latitude']].apply(lambda x: round(pd.to_numeric(x, errors = 'coerce'), 1))
        stores.dropna(subset = ['staff_numbers'], inplace=True)
        stores = stores.astype({'staff_numbers': 'int64'})

        # Dropping nan values, redundant index column, lat column which contains little valid data and resetting index
        stores.drop(['index', 'lat'], axis = 1, inplace=True)
        stores.dropna(inplace = True, subset = ['store_code', 'store_type'])
        # 'lat' column is largely populated with np.nan so it is important it is deleted prior to dropna unless subset is used here, thresh parameter defines number of non-nan values to keep the column, could also use a subset here based on store type or store code
        stores.drop_duplicates(inplace = True)
        stores.reset_index(drop = True, inplace=True)

        return stores

    
    def convert_product_weights(self, df):
                
        products = df.copy()

        products.weight =  products.weight.apply(lambda x: str(x)[: (str(x).index('g')+1)] if 'g' in str(x) else x)

        products.weight =  products.weight.apply(lambda x:
        pd.to_numeric(str(x)[:-2], errors = 'coerce') if str(x)[-2:] == 'kg' else
        (0.001*pd.to_numeric(str(x).strip('g').split(' ')[0]) * pd.to_numeric(str(x).strip('g').split(' ')[-1]) if ('x' in str(x) and str(x)[-1] == 'g') else 
        (0.001*pd.to_numeric(str(x)[:-1], errors = 'coerce') if str(x)[-1] == 'g' else
        (0.001*pd.to_numeric(str(x)[:-2], errors = 'coerce') if str(x)[-2:] == 'ml' else
        (0.0284*pd.to_numeric(str(x)[:-2], errors = 'coerce') if str(x)[-2:] == 'oz' else 
        np.nan)))))

        products.weight =  products.weight.apply(lambda x: round(x, 2))

        return products


    def clean_product_data(self, df):

        products = df.copy()

        # Verify category column against valid categories
        categories = ['homeware', 'toys-and-games', 'food-and-drink', 'pets', 'sports-and-leisure', 'health-and-beauty', 'diy']
        products.category =  products.category.apply(lambda x: x if x in categories else np.nan)

        # Verify removed column against valid availability categories
        availability = ['Still_avaliable', 'Removed']
        products.removed = products.removed.apply(lambda x: x if x in availability else np.nan).replace('Still_avaliable', 'Still_available')

        # Convert date added column entries to datetime date type
        products.date_added = products.date_added.apply(lambda x: pd.to_datetime(x, format = '%Y-%m-%d' , errors = 'coerce'))

        # Conveert price column to numeric data
        products.product_price = products.product_price.apply(lambda x: round(pd.to_numeric(str(x).strip('£'), errors='coerce'), 2))

        # Verify that uuid column entires follow a specific format
        products.uuid = products.uuid.apply(lambda x: x if re.match('^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', str(x)) else np.nan)

        # Verify that EAN column entries follow a specific format
        products.rename(columns = {'EAN': 'ean'}, inplace=True)
        products.ean = products.ean.apply(lambda x: str(x) if re.match('^[0-9]{12,13}$', str(x)) else np.nan)
         
        # Verify that product_code column entries follow a specific format
        products.product_code = products.product_code.apply(lambda x: x if re.match('^[a-zA-Z0-9]{2}-[0-9]{6,7}[a-zA-Z]$', str(x)) else np.nan)

        # Delete unnecessary columns, nan entries and reset index (no duplicates confirmed)
        products.drop('Unnamed: 0', axis=1, inplace=True)
        products.dropna(inplace = True, subset=['product_code'])
        products.drop_duplicates(inplace=True)
        products.reset_index(drop=True, inplace=True)
            
        return products


    def clean_orders_data(self, df):
        
        orders = df.copy()
        
        # Drop redundant columns as instructed
        orders = orders.drop(['first_name', 'last_name', '1', 'level_0', 'index'], axis=1)

        # Invalid entries containing alphabetical characters are removed from the card_number column; column entries are also checked against the expected correct number of digits
        orders.card_number.replace('[\D]', '', regex=True, inplace=True) 
        orders.card_number = orders.card_number.apply(lambda x: str(x) if len(str(x)) in range(12,20) else np.nan) # NOTE there seem to be a lot of cards with 11 no. digits (800+) which appear invalid

        # Convert product quantity column to numeric data type
        orders.product_quantity = orders.product_quantity.apply(lambda x: pd.to_numeric(x, errors = 'coerce')).astype('int64')

        # Verify that date_uuid and user_uuid column entries follow a specific format
        orders.user_uuid = orders.user_uuid.apply(lambda x: x if re.match('^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', str(x)) else np.nan)
        orders.date_uuid = orders.date_uuid.apply(lambda x: x if re.match('^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', str(x)) else np.nan)
         # orders[['user_uuid', 'date_uuid']] = orders[['user_uuid', 'date_uuid']].apply(lambda x: x if re.match('^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', str(x)) else np.nan) # Why does this line not work?

        # Verify that store_code and product code columns entries follow a specific format
        orders.store_code = orders.store_code.apply(lambda x: str(x) if re.match('^[A-Z]{2,3}-[A-Z0-9]{8}$', str(x)) else np.nan)
        orders.product_code = orders.product_code.apply(lambda x: str(x) if re.match('^[a-zA-Z][0-9]-[0-9]{5,7}[a-zA-Z]$', str(x)) else np.nan)

        # Remove rows with nan entries and duplicates and reset index
        # orders.dropna(inplace=True) # currently keep all rows to avoid over-cleaning
        orders.drop_duplicates(inplace=True)
        orders.reset_index(drop=True, inplace=True)

        return orders


    def clean_date_data(self, df):
        
        dates = df.copy()

        # Denote NULL entries as format recognizable by pandas / numpy
        dates.replace('NULL', np.nan, inplace=True)

        # Convert timestamp column to datatime.time format
        dates['timestamp'] = dates['timestamp'].apply(lambda x: pd.to_datetime(x, format= '%H:%M:%S', errors = 'coerce')).dt.time

        # Clean up months column by removing invalid entries
        months = [*range(1,13)]
        dates.month = dates.month.apply(lambda x: x if pd.to_numeric(x, errors='coerce') in months else np.nan)

        # Clean up days column by removing invalid entries
        days = [*range(1,32)]
        dates.day = dates.day.apply(lambda x: x if pd.to_numeric(x, errors='coerce') in days else np.nan)

        # Clean up years column by removing invalid entries
        years = [*range(1980,2023)]
        dates.year = dates.year.apply(lambda x: x if pd.to_numeric(x, errors='coerce') in years else np.nan)

        # Clean up user uuid column by removing entries of invalid length
        dates.date_uuid = dates.date_uuid.apply(lambda x: x if re.match('^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', str(x)) else np.nan)

        # Clean up periods column by removing invalid entries
        periods = ['Morning', 'Midday', 'Evening', 'Late_Hours']
        dates.time_period = dates.time_period.apply(lambda x: x if x in periods else np.nan)

        # Remove nan values, duplicates and reset index
        dates.dropna(inplace = True, subset=['date_uuid']) # currently keep all rows to avoid over-cleaning
        dates.drop_duplicates(inplace=True)
        dates.reset_index(drop=True, inplace=True)

        return dates


# USERS
# df_users = DataExtractor().extract_rds_table(DatabaseConnector(), 'legacy_users')
# users = DataClean().clean_user_data(df_users)

# FURTHER NOTES ON USERS CLEANING:
# Cross-check country entry against country code entry and turn into categorical data
# NOTES: email addresses look ok (one duplicated!!), but you might want to check for @? difficult to make bullet-proof, same goes for address although could make sure it includes /
# users.email_address.replace('^[.+]+@+[.+]', np.nan, regex=True, inplace=True)

#CARDS
# df_cards = DataExtractor().retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
# cards = DataClean().clean_card_data(df_cards)

# STORES
# df_stores = DataExtractor().retrieve_stores_data("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}", {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"})
# stores = DataClean().clean_stores_data(df_stores)

# FURTHER NOTES ON STORES CLEANING
# could cross-check country code against continent

# PRODUCTS
# df_products = DataExtractor().extract_from_s3('s3://data-handling-public/products.csv')
# products = DataClean().clean_product_data(DataClean().convert_product_weights(df_products))

# FURTHER NOTES ON PRODUCTS CLEANING
# not clear on why you would convert data into kg instead of g, a lot of data reduced to 0 currently

# ORDERS
# df_orders = DataExtractor().extract_rds_table(DatabaseConnector(), 'orders_table')
# orders = DataClean().clean_orders_data(df_orders)

# DATES
# df_dates = DataExtractor().extract_json_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
# dates = DataClean().clean_date_data(df_dates)

# FURTHER NOTES ON DATES CLEANING
# Cross-check timestamp against time_period
