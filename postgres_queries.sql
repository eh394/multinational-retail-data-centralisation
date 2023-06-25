-- Manipulate Data
-- TASK 1 UPDATE ORDERS_TABLE
ALTER TABLE orders_table
ALTER COLUMN date_uuid SET DATA TYPE UUID USING date_uuid::UUID,
ALTER COLUMN user_uuid SET DATA TYPE UUID USING user_uuid::UUID,
ALTER COLUMN card_number TYPE VARCHAR(20),
ALTER COLUMN store_code TYPE VARCHAR(12),
ALTER COLUMN product_code TYPE VARCHAR(11),
ALTER COLUMN product_quantity TYPE SMALLINT; 

-- TASK 2 UPDATE DIM_USERS
ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255),
ALTER COLUMN last_name TYPE VARCHAR(255),
ALTER COLUMN date_of_birth TYPE DATE,
ALTER COLUMN country_code TYPE VARCHAR(2),
ALTER COLUMN user_uuid SET DATA TYPE UUID USING user_uuid::UUID,
ALTER COLUMN join_date TYPE DATE; 

-- TASK 3 UPDATE DIM_STORE_DETAILS
ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT,
ALTER COLUMN locality TYPE VARCHAR(255),
ALTER COLUMN store_code TYPE VARCHAR(11),
ALTER COLUMN staff_numbers TYPE SMALLINT,
ALTER COLUMN opening_date TYPE DATE,
ALTER COLUMN store_type TYPE VARCHAR(255),
ALTER COLUMN latitude TYPE FLOAT,
ALTER COLUMN country_code TYPE VARCHAR(2),
ALTER COLUMN continent TYPE VARCHAR(255); 

-- TASK 4 MAKE CHANGES TO DIM_PRODUCTS
ALTER TABLE orders_table
ADD COLUMN location VARCHAR(20); 
    
UPDATE dim_products
SET weight_class = 'Light'
WHERE weight < 2; 

UPDATE dim_products
SET weight_class = 'Mid_Sized'
WHERE weight >= 2 AND weight < 40.5; 

UPDATE dim_products
SET weight_class = 'Heavy'
WHERE weight >= 40.5 AND weight < 140.5; 

UPDATE dim_products
SET weight_class = 'Truck_Required'
WHERE weight >= 140.5; 

ALTER TABLE dim_products
RENAME COLUMN removed TO still_available; 

-- TASK 5 UPDATE DIM_PRODUCTS
ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT,
ALTER COLUMN weight TYPE FLOAT,
ALTER COLUMN ean TYPE VARCHAR(13), # note that this does not seem to work if column name is in uppercase
ALTER COLUMN product_code TYPE VARCHAR(11),
ALTER COLUMN date_added TYPE DATE,
ALTER COLUMN uuid SET DATA TYPE UUID USING uuid::UUID,
ALTER COLUMN weight_class TYPE VARCHAR(255); 

ALTER TABLE dim_products
ALTER COLUMN still_available TYPE BOOL
USING CASE
	WHEN still_available = 'Still_available' THEN true
	WHEN still_available = 'Removed' THEN false
	ELSE NULL
END; 

-- TASK 6 UPDATE DIM_DATE_TIMES
ALTER TABLE dim_date_times
ALTER COLUMN month TYPE CHAR(2),
ALTER COLUMN year TYPE CHAR(4),
ALTER COLUMN day TYPE CHAR(2),
ALTER COLUMN time_period TYPE VARCHAR(255),
ALTER COLUMN date_uuid SET DATA TYPE UUID USING date_uuid::UUID; 

-- TASK 7 UPDATE DIM_CARD_DETAILS
ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(20),
ALTER COLUMN expiry_date TYPE VARCHAR(10),
ALTER COLUMN date_payment_confirmed TYPE DATE; 

-- TASK 8 SET PRIMARY KEYS
ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number); 

ALTER TABLE dim_date_times
ADD PRIMARY KEY (date_uuid); 

ALTER TABLE dim_products
ADD PRIMARY KEY (product_code); 

ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code); 

ALTER TABLE dim_users
ADD PRIMARY KEY (user_uuid); 

-- TASK 9 SET FOREIGN KEYS
ALTER TABLE orders_table
ADD FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number) # currently does not run

ALTER TABLE orders_table
ADD FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid)

ALTER TABLE orders_table
ADD FOREIGN KEY (product_code) REFERENCES dim_products(product_code) # currently does not run

ALTER TABLE orders_table
ADD FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code) # currently does not run

ALTER TABLE orders_table
ADD FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid) # currently does not run

-- Query Data
-- TASK 1
SELECT country_code, COUNT(*) FROM dim_store_details
GROUP BY country_code; 

-- TASK 2
SELECT locality, COUNT(*) FROM dim_store_details
GROUP BY 1
ORDER BY 2 DESC;
 
-- TASK 3
WITH transaction_spend AS(
	SELECT dim_date_times.month,
	(dim_products.product_price * orders_table.product_quantity) AS amount_spent
	FROM orders_table
	JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
	JOIN dim_products ON orders_table.product_code = dim_products.product_code
	)
	
SELECT ROUND(SUM(amount_spent)), month
FROM transaction_spend
GROUP BY 2
ORDER BY 1 DESC; 

-- TASK 4
WITH store_information AS (
	SELECT store_code,
	CASE
		WHEN store_code LIKE 'WEB%' THEN 'Online'
		ELSE 'Offline'
	END AS location
	FROM dim_store_details
	)
	
SELECT COUNT(*), store_information.location FROM orders_table
JOIN store_information on orders_table.store_code = store_information.store_code
GROUP BY store_information.location


UPDATE orders_table
ADD COLUMN location VARCHAR(20)

UPDATE orders_table
SET location = 'Web'
WHERE store_code LIKE 'WEB%'


UPDATE orders_table
SET location = 'Offline'
WHERE store_code NOT LIKE 'WEB%' # could use CASE statement here


SELECT location, 
SUM(product_quantity) AS product_quantity_count, 
COUNT(*) AS numer_of_sales 
FROM orders_table
GROUP BY location; 
# Results slightly differ from what was given, but relatively small difference

-- TASK 5
WITH 
sales_per_store_type AS (
SELECT (orders_table.product_quantity * dim_products.product_price) AS total_sales,
dim_store_details.store_type
FROM orders_table
JOIN dim_products ON orders_table.product_code = dim_products.product_code
JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
),
total_sales_per_store_type AS (
SELECT store_type, 
SUM(total_sales) AS total_sales
FROM sales_per_store_type
GROUP by 1
ORDER BY 2 DESC
),
percentage_total_sales_per_store_type AS (
SELECT store_type, 
total_sales,
100 * total_sales / (SELECT SUM(total_sales) FROM total_sales_per_store_type) AS percentage_total
FROM total_sales_per_store_type
GROUP BY 1, 2
ORDER BY 2 DESC
)

SELECT store_type, 
ROUND(total_sales) AS total_sales, 
ROUND(CAST(percentage_total AS numeric), 2) AS percentage
FROM percentage_total_sales_per_store_type

-- TASK 6
WITH sales_per_month_year AS (
SELECT (orders_table.product_quantity * dim_products.product_price) AS total_sales,
dim_date_times.year, dim_date_times.month
FROM orders_table
JOIN dim_products ON orders_table.product_code = dim_products.product_code
JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
)

SELECT ROUND(SUM(total_sales)) as total_sales,
year, month
FROM sales_per_month_year
GROUP BY 3, 2
ORDER BY 1 DESC; 

-- Alternatively:
WITH individual_sales AS (
SELECT (orders_table.product_quantity * dim_products.product_price) AS total_sales,
dim_date_times.year, dim_date_times.month
FROM orders_table
JOIN dim_products ON orders_table.product_code = dim_products.product_code
JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
),
monthly_sales AS (
SELECT ROUND(SUM(total_sales)) AS tot_sales_month,
year, month
FROM individual_sales
GROUP BY 3, 2
ORDER BY 2, 3
),
annual_max AS (
SELECT MAX(tot_sales_month) AS max_total_sales_month, year
FROM monthly_sales
GROUP BY 2
ORDER BY 1 DESC
)

SELECT annual_max.max_total_sales_month,
annual_max.year, monthly_sales.month
FROM annual_max
JOIN monthly_sales ON annual_max.max_total_sales_month = monthly_sales.tot_sales_month
ORDER BY 1 DESC; 

-- TASK 7
SELECT SUM(staff_numbers), country_code
FROM dim_store_details
GROUP BY 2
ORDER BY 1 DESC;

-- TASK 8 
WITH sales_per_store_type_country AS (
SELECT (orders_table.product_quantity * dim_products.product_price) AS sales,
dim_store_details.store_type,
dim_store_details.country_code
FROM orders_table
JOIN dim_products ON orders_table.product_code = dim_products.product_code
JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
)

SELECT ROUND(CAST(SUM(sales) AS NUMERIC), 0) AS total_sales, # you need to use CAST here if you want to be able to specify number of SFs. Otherwise you can still use ROUND, but it will default to 0 SFs. 
store_type, country_code
FROM sales_per_store_type_country
GROUP BY 2, 3
HAVING country_code = 'DE'
ORDER BY 1; 

SELECT COUNT(*) AS total_sales,
dim_store_details.store_type,
dim_store_details.country_code
FROM orders_table
JOIN dim_products ON orders_table.product_code = dim_products.product_code
JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY 2, 3
HAVING country_code = 'DE'
ORDER BY 1; 

-- TASK 9
WITH original_time_info AS (
	SELECT dim_date_times.year,
	dim_date_times.month,
	dim_date_times.day,
	dim_date_times.timestamp
	FROM dim_date_times
	JOIN orders_table ON dim_date_times.date_uuid = orders_table.date_uuid
	ORDER BY 1,2,3,4
	),
	full_datetime AS (
	SELECT
	year, month, day, timestamp,
	MAKE_DATE(CAST(year AS int), CAST(month AS int), CAST(day AS int)) + timestamp AS complete_datetime
	FROM original_time_info
	),
	time_intervals AS (
	SELECT year, month, day,
	LEAD(complete_datetime) 
		OVER (ORDER BY year, month, day) - complete_datetime AS difference
	FROM full_datetime
	)
	
SELECT year, AVG(difference)
FROM time_intervals
GROUP BY 1
ORDER BY 2 DESC; 