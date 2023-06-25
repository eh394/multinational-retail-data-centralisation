import yaml
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd
import tabula
import requests
import json
import boto3


class DataExtractor:
    
    def extract_rds_table(self, instance, table_name): # is there a reason you pass instance here rather than engine (creating engine twice)
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
        # convert json to dictionary, then create a list of dictionaries, then convert into a pandas dataframe
        # dict_list = []
        # for i in range(451):
        #     r = requests.get(retrieve_a_store_endpoint.format(store_number = i), headers = header_dictionary)
        #     dict_list.append(r.json())
        # df = pd.DataFrame(dict_list)
        # df.to_csv('store_details.csv')
        df = pd.read_csv('store_details.csv', index_col = 0, on_bad_lines = 'skip')
        return df

    def extract_from_s3(self, s3_address):
        df = pd.read_csv(s3_address)
        return df

    def extract_json_data(self, s3_address):
        df = pd.read_json(s3_address)
        return df


