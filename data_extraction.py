import yaml
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd
import tabula
import requests
import json
import boto3
# from database_utils import DatabaseConnector



class DataExtractor:
    
    def extract_rds_table(self, instance, table_name):
        engine = instance.init_db_engine()
        df = pd.read_sql_table(table_name, engine)
        return df

    def retrieve_pdf_data(self, link):
        # tabula.convert_into(link, 'card_details.csv', pages = 'all')
        df = pd.read_csv('card_details.csv', index_col = 0, skiprows = 1, on_bad_lines = 'skip')
        return df

    def list_number_of_stores(self, number_of_stores_endpoint, header_dictionary):
        r = requests.get(number_of_stores_endpoint, headers = header_dictionary)
        print(r.text)
        return r

    def retrieve_stores_data(self, retrieve_a_store_endpoint, header_dictionary):
        # convers json to dictionary, then create a list of dictionaries, then convert into a pandas dataframe
        # dict_list = []
        # for i in range(451):
        #     r = requests.get(retrieve_a_store_endpoint.format(store_number = i), headers = header_dictionary)
        #     r_json = r.json()
        #     dict_list.append(r_json)
        # df = pd.DataFrame(dict_list)
        # df.to_csv('store_details.csv')
        df = pd.read_csv('store_details.csv', index_col = 0, on_bad_lines = 'skip')
        # print(df.head(10))
        return df

    def extract_from_s3(self, s3_address):
        df = pd.read_csv(s3_address)
        # print(df.head())
        return df

    def extract_json_data(self, s3_address):
        df = pd.read_json(s3_address)
        # print(df.head())
        return df


# DataExtractor().extract_json_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json') 


# DataExtractor().extract_from_s3('s3://data-handling-public/products.csv')

# DataExtractor().retrieve_stores_data("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}", {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"})
# DataExtractor().list_number_of_stores("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores", {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"})
# number_of_stores = 451

# print(DataExtractor().extract_rds_table(DatabaseConnector(), 'legacy_users'))

# pdf_df = DataExtractor().retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
# print(pdf_df.head(20))
# print(pdf_df.info())
