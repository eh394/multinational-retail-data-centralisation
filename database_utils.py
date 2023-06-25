import yaml
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd
from data_extraction import DataExtractor

class DatabaseConnector:
    
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as db_creds_file:
            db_creds = yaml.safe_load(db_creds_file)
            return db_creds

    # creates engine for the AWS RDS database which some of the original uncleaned dataframes such as users are sourced from 
    def init_db_engine(self):
        db_creds = self.read_db_creds()
        engine = create_engine(f"{db_creds['DATABASE_TYPE']}+{db_creds['DBAPI']}://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}")
        return engine
    
    def list_db_tables(self):
        engine = self.init_db_engine()
        inspector = inspect(engine)
        for table in inspector.get_table_names():
            print(table)

    # creates engine for the local Postgres database where all cleaned data gets uploaded to
    def upload_to_db(self, dataframe, table_name):
        db_creds = self.read_db_creds()
        engine = create_engine(f"{db_creds['DATABASE_TYPE']}+{db_creds['DBAPI']}://{db_creds['USER']}:{db_creds['PASSWORD']}@{db_creds['HOST']}:{db_creds['PORT']}/{db_creds['DATABASE']}")
        dataframe.to_sql(table_name, con = engine, if_exists = 'replace') # use 'replace' if want to overwrite the existing table, 'fail' if you want to throw an error when attempting to overwrite  






