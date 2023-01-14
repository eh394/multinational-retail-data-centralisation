# The repository comprises several files:

1. database_utils.py
This file contains DataConnector class and the class itself contains four methods.
read_db_creds: opens a yaml file in a read mode and saves database credentials saved in this file into a dictionary (the yaml file itself is denoted as dictionary). The method returns this dictionary.

init_db_engine: this method uses the database credentials returned from the read_db_credentials to create an engine, a connection, to AWS RDS database. Note that this is the database some of the original, pre-cleaning, data is stored. This method returns the engine itself.

list_db_tables: this method takes the initialised engine from the init_db_engine method and creates an inspector object which then utilises .get_table_names() method to print out table names of all tables in this AWS RDS database. Note that there are 3 no. tables in this database.

upload_to_db: this method uploads the cleaned data to the Sales_Data database, local postres database to be further analysed. Note that it first uses 
read_db_creds: method to read database credentials (which are stored alongside AWS RDS database credentials in the same yaml file), then creates an engine and then uploads it to the local postgres database. Note that the parameter in the upload line if_exists defines whether the dataframe gets overwritten if one already exists. This is currently left as 'replace' which results in overwriting the existing database. 

2. data_extraction.py
This file contains DataExtractor class and the class itself contains six methods.

extract_rds_table: this method takes an instance of the DataConnector class and recreates the engine connecting to the AWS RDS database. It then uses .read_sql_table to save a table from this database to a pandas dataframe. The method returns this dataframe.

retrieve_pdf_data: this method uses tabula module first to convert pdf data saved in a link to a csv file. Note that this line is commented out as this process needs to happen only once. The method then saves this csv data to a pandas dataframe and returns this dataframe.

list_number_of_stores: the next two methods fetch data through use of API. This method retrieves number of stores the data covers. The result is 451 stores.

retrieve_stores_data: this method uses a for loop to send a request via API 451 times to retrieve stores data. Each request is turned into a json file and then appended to an empty list which becomes a list of dictionaries. This is then converted into a pandas dataframe and then saved to a csv file locally. Note that this entire segment of code is commented out as this only needs to happen once. The method then saves the csv file back into a pandas dataframe. The method returns this dataframe.

extract_from_s3: this method extracts data from AWS s3 bucket. As data is loaded as csv format, this is then converted into a pandas dataframe. The method returns this dataframe.

extract_json_data: this method works similarly to the above, except it works with json files rather than csv files.

3. data_cleaning.py
This file contains DataClean class and the class itself contains seven methods. Six of the methods clean data belonging to a particular dataframe, namely: users, cards, stores, products, orders, and dates. In addition to this, there is another method which specifically deals with cleaning weight data in the products database. 
In general, all methods carry out the following:
- replace 'NULL' values with a valid pandas syntax, here np.nan instead
- verify that where applicable, data is following a specific format, for instance EAN number present in products database comprises of 12-13 digits
- verify that where applicable, categorical data is valid (e.g. store_type in stores dataframe)
- convert data into datetime data format where applicable
- convert data to numeric where applicable
- remove redundant columns (often index)
- remove rows with nan values
- remove duplicates
- reset index
Note that all of the above has been carried out with care to ensure siginificant volume of data is not lost. In all cases data removed is within approx. 1% of the original dataframe.

4. data_main.py
This file executes the code from the three remaining files. It calls DataExtract and DataConnect classes to extract the original data, DataClean class to clean it and DataConnect again to upload them to a local postgres dataframe.