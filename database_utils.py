import yaml
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd
from data_cleaning import DataClean
from data_extraction import DataExtractor

class DatabaseConnector:
    
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as db_creds_file:
            db_creds = yaml.safe_load(db_creds_file)
            return db_creds

    def init_db_engine(self):
        db_creds = self.read_db_creds()
        engine = create_engine(f"{db_creds['DATABASE_TYPE']}+{db_creds['DBAPI']}://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}")
        return engine
    
    def list_db_tables(self):
        engine = self.init_db_engine()
        inspector = inspect(engine)
        for table in inspector.get_table_names():
            print(table)

    def upload_to_db(self, dataframe, table_name):
        db_creds = self.read_db_creds()
        engine = create_engine(f"{db_creds['DATABASE_TYPE']}+{db_creds['DBAPI']}://{db_creds['USER']}:{db_creds['PASSWORD']}@{db_creds['HOST']}:{db_creds['PORT']}/{db_creds['DATABASE']}")
        dataframe.to_sql(table_name, con = engine, if_exists = 'fail') # use 'replace' if want to overwrite the existing table 



connector = DatabaseConnector()


# df_dates = DataExtractor().extract_json_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
# connector.upload_to_db(DataClean().clean_date_data(df_dates), 'dim_date_times')

# df_orders = DataExtractor().extract_rds_table(connector, 'orders_table')
# connector.upload_to_db(DataClean().clean_orders_data(df_orders), 'orders_table')

# df_products = DataExtractor().extract_from_s3('s3://data-handling-public/products.csv')
# connector.upload_to_db(DataClean().clean_product_data(DataClean().convert_product_weights(df_products)), 'dim_products')

# df_users = DataExtractor().extract_rds_table(connector, 'legacy_users')
# connector.upload_to_db(DataClean().clean_user_data(df_users), 'dim_users')

# df_cards = DataExtractor().retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
# connector.upload_to_db(DataClean().clean_card_data(df_cards), 'dim_card_details')

# df_stores = DataExtractor().retrieve_stores_data("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}", {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"})
# connector.upload_to_db(DataClean().clean_stores_data(df_stores), 'dim_store_details')



# DatabaseConnector().list_db_tables()
# DatabaseConnector().upload_to_db(users, dim_users)


