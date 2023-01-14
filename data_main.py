from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from data_cleaning import DataClean


# CREATE A CONNECTOR
connector = DatabaseConnector()

# EXTRACT, CLEAN AND UPLOAD USERS DATAFRAME
# Initial data stored in AWS RDS Database
# List tables contained within the AWS RDS Database
# DatabaseConnector().list_db_tables()
df_users = DataExtractor().extract_rds_table(connector, 'legacy_users')
connector.upload_to_db(DataClean().clean_user_data(df_users), 'dim_users')

# EXTRACT, CLEAN AND UPLOAD CARDS DATAFRAME
# Initial data stored in a PDF document in an AWS S3 bucket
df_cards = DataExtractor().retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
connector.upload_to_db(DataClean().clean_card_data(df_cards), 'dim_card_details')

# EXTRACT, CLEAN AND UPLOAD STORES DATAFRAME
# Data extracted through the use of API
# List number of stores in the database
# DataExtractor().list_number_of_stores("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores", {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"})
# number_of_stores = 451
df_stores = DataExtractor().retrieve_stores_data("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}", {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"})
connector.upload_to_db(DataClean().clean_stores_data(df_stores), 'dim_store_details')

# EXTRACT, CLEAN AND UPLOAD PRODUCTS DATAFRAME
# Initial data stored in a CSV file in an AWS S3 bucket
df_products = DataExtractor().extract_from_s3('s3://data-handling-public/products.csv')
connector.upload_to_db(DataClean().clean_product_data(DataClean().convert_product_weights(df_products)), 'dim_products')

# EXTRACT, CLEAN AND UPLOAD ORDERS DATAFRAME
# Initial data stored in AWS RDS Database
df_orders = DataExtractor().extract_rds_table(connector, 'orders_table')
connector.upload_to_db(DataClean().clean_orders_data(df_orders), 'orders_table')

# EXTRACT, CLEAN AND UPLOAD DATES DATAFRAME
# Initial data stored in a JSON file in an AWS S3 bucket
df_dates = DataExtractor().extract_json_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
connector.upload_to_db(DataClean().clean_date_data(df_dates), 'dim_date_times')

