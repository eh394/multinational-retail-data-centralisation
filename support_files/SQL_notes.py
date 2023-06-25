# ERD - Entity Relationship Diagram

# JOINING TABLES ON A COLUMN

SELECT * FROM table_1 # instead of * you can specify column names and as below you can select columns from both tables as long as you use a prefix; you can aslo alias tables to keep it simpler, e.g. specify 'table_1 AS t1' here and below 'JOIN table_2 AS T2 below; you could also remove keyword AS from this command and it would still run correcttly, but arguably it would be less clear
JOIN table_2
ON table_2.column_name_2 = table_1.column_name_1
WHERE column_name_2 = 'Insert Relevant Input'

# More complex example of join on three tables is included below
SELECT * FROM address
JOIN city
ON address.city_id = city.city_id
JOIN country
ON city.country_id = country.country_id
WHERE country = 'United Kingdom'

# The default is an inner join. Types of joins are specified below where first table is referred to as table 2 and second one as table 2:
# inner join: only overlap, i.e. common entries of tables 1 and 2
# left join: overlap plus all of table 1
# right join: overlap plus all of table 2
# outer join: both tables entirely
LEFT JOIN
RIGHT JOIN
FULL JOIN
# would need to replace the 'JOIN' in the block above
# Other join examples below:
SELECT * FROM a
LEFT JOIN b ON a.key = b.key
WHERE b.key IS NULL # returns table a MINUS the overlap between tables a and b, you can achieve the reverse with a right join or swapping table names

SELECT * FROM a
FULL JOIN b ON a.key = b.key
WHERE a.key IS NULL or b.key IS NULL # returns all but the overlap, i.e. tables a and b minus the parts of these tables that are in common

# AGGREGATIONS
# Aggregation Functions
COUNT # how many rows are in a particular column (or columns)
SUM # add all values in a particular column
MIN # lowest value in a particular column
MAX # highest value in a particular column
AVG # average value in a particular column
GROUP BY 
HAVING

# Functions
DISTINCT # SELECT DISTINCT returns unique instances over a column, used over multiple columns it evaluates unique combinations
SELECT DISTINCT rental_rate, special_features
FROM film; 

DATE_TRUNC # SELECT DATE_TRUNC will truncate the date to reduce it from the standard format of YYYY-MM-DD HH:MM:SS to a chosen parameter, example below
SELECT DATE_TRUNC('day', payment_date) AS day, COUNT(*) FROM payment
GROUP BY 1
ORDER BY 1; 

DATE_PART # retrieves subfields such a year (YEAR), month (MONTH) or hour (HOUR) from date/time values, you can also extract day of the week (DOW), day of the year (DOY) or even millennium (MILLENIUM)
SELECT DATE_PART('DOW', payment_date) AS day, COUNT(*) FROM payment
GROUP BY 1; 

# Flow Control
CASE # creates a new column based on the conditions we declare, it has to include keywords such as WHEN, THEN, END and optionally ELSE
SELECT title, release_year, rental_rate
CASE 
    WHEN rental_rate > 0 AND rental_rate < 2.99 THEN 'discount' 
    WHEN rental_rate >= 2.99 AND rental_rate < 4.99 THEN 'regular' 
    ELSE 'premium'
END AS quality
FROM film

NULLIF # 

# Example
SELECT rental_rate, rating, COUNT(*) FROM film
GROUP BY rating, rental_rate
HAVING COUNT(*) > 60
# the above returns number of films (COUNT(*)) with a given rental_rate and rating for more than 60 entries

# GROUP BY syntax
SELECT {column}, {aggregation} FROM {table}
GROUP BY {column}
HAVING {aggregation_condition}
# when using aggregates, any column in the SELECT statement which is not an aggregate, must also be specified in the GROUP BY statement (does not have to be the other way around)

SELECT customer_id, COUNT(*)
FROM rental
GROUP BY customer_id
ORDER BY 1; 

# ORDER OF OPERATIONS BEING CARRIED OUT
FROM + JOIN --> WHERE --> GROUP BY --> HAVING --> SELECT --> ORDER BY --> LIMIT
# NOTE: The above is important, for instance regarding where you alias your column. For instance, if you alias your column in the SELECT operation, the alias will not be valid in the HAVING operation

# WITH statement: allows to include previous result in your query, example below
WITH previous_results AS(
	SELECT DATE_TRUNC('day', payment_date) AS day, SUM(amount) AS day_sum
	FROM payment
	GROUP BY 1
	ORDER BY 2 DESC
	)
SELECT DATE_PART('DOW', day) AS dow, AVG(day_sum) AS dow_average
FROM previous_results
GROUP BY 1
ORDER BY 2 DESC;

# SUB-QUERIES
# This effectively relates to nested queries, see example below
SELECT * FROM actor WHERE actor_id IN (
    SELECT actor_id
    FROM film_actor
    WHERE film_id = (
        SELECT film_id FROM film WHERE title = 'DRAGON SQUAD'
    )
)

# example
SELECT * FROM film WHERE film_id IN (
	SELECT film_id FROM film_category WHERE category_id IN (
		SELECT category_id FROM category WHERE name = 'Animation'	
		)
	); 

# example no. 2
SELECT first_name, last_name, email FROM customer WHERE address_id IN (
	SELECT address_id FROM address WHERE city_id IN (
		SELECT city_id FROM city WHERE country_id IN (
			SELECT country_id FROM country WHERE country = 'Canada'	
			)
		)
	); 

# example no. 3
SELECT title FROM film WHERE (title LIKE 'A%' OR title LIKE 'I%') AND title IN (
	SELECT title FROM film WHERE language_id IN (
			SELECT language_id FROM language WHERE name != 'Italian' OR name != 'French' OR name != 'Spanish'
			)
		); 

# example no. 4
WITH previous_results AS (
	SELECT COUNT(payment_id), staff_id, DATE_PART('DAY', payment_date) AS day
	FROM payment
	GROUP BY 2, 3
	)
	
SELECT AVG(count), staff_id
FROM previous_results
GROUP BY staff_id;

# example no. 5
SELECT film.title, SUM(rental.rental_id) AS number_of_rentals FROM rental
JOIN inventory ON rental.inventory_id = inventory.inventory_id
JOIN film ON inventory.film_id = film.film_id
GROUP BY 1
ORDER BY 2 DESC;



# WITH command allows the previous results to be used / referrenced later in the query eliminating need for nested commands and also adding clarity to the queries - this is referred to as CTE - COMMON TABLE EXPRESSIONS
# NOTE that the below does not yet account for the fact that customers may have made multiple rentals or varying amounts at the same time stamp. i.e. the data does not seem to return 'total' orders per customers, but instead a series of indidivual transactions
WITH previous_results AS(
	SELECT customer.customer_id, customer.first_name, customer.last_name, 
	rental.rental_date, 
	payment.amount 
	FROM customer
	JOIN rental
	ON customer.customer_id = rental.customer_id
	JOIN payment
	ON customer.customer_id = payment.customer_id
	)
SELECT customer_id, first_name, last_name, amount, MIN(rental_date) AS min_rental_date
FROM previous_results 
GROUP BY customer_id, first_name, last_name, amount; 

# returns number of films (or rental ids in this case) which do not have a registered return date
SELECT COUNT(*)
FROM rental
WHERE return_date IS NULL;

# example of WHERE statement used with an aggregate function - investigate
SELECT SUM(amount)
FROM payment
WHERE payment_date BETWEEN '25-01-2007' AND '29-01-2007'; 

# example of a more complex query combining multiple tables
SELECT film.rating, AVG(payment.amount)
FROM film
JOIN inventory ON film.film_id = inventory.film_id
JOIN rental ON inventory.inventory_id = rental.inventory_id
JOIN payment ON rental.rental_id = payment.rental_id
GROUP BY film.rating; 

# another example
SELECT film.title, inventory.film_id, COUNT(*) FROM inventory
JOIN film ON film.film_id = inventory.film_id
WHERE film.title = 'HUNCHBACK IMPOSSIBLE'
GROUP BY film.title, inventory.film_id; # do not forget the necessity to add the GROUP By statement even though you are returning a single line

# another example
SELECT staff.first_name, staff.last_name,
address.address, SUM(payment.amount)
FROM payment
JOIN staff ON payment.staff_id = staff.staff_id
JOIN store ON staff.store_id = store.store_id
JOIN address ON store.address_id = address.address_id
GROUP BY staff.first_name, staff.last_name, address.address; 

# NOTE this example would not return anything as there does not appear to be a customer who spent over 150 USD - IS THIS CORRECT?
SELECT payment.customer_id, city.city, SUM(payment.amount) FROM city
JOIN address ON city.city_id = address.city_id
JOIN staff ON address.address_id = staff.address_id
JOIN payment ON staff.staff_id = payment.staff_id
GROUP BY payment.customer_id, city.city
HAVING SUM(payment.amount) > 150
ORDER BY city; 

# another example
WITH previous_results AS(
	SELECT
		CASE
		WHEN amount < 3 THEN 'low'
		WHEN amount >= 3 AND amount <=7 THEN 'medium'
		ELSE 'high'
	END AS payment_category
	FROM payment
	)

SELECT COUNT(*), payment_category
FROM previous_results
GROUP BY payment_category; 

# adds a new column to a table
ALTER TABLE table_name
ADD COLUMN colum_name TEXT; 


column_name DATA_TYPE OTHER 
DATA_TYPE: TEXT, INTEGER, 
OTHER: NULL, NOT NULL, UNIQUE, PRIMARY KEY, DEFAULT 'Some text', 


# MILESTONE 3 WORK IN PROGRESS QUERIES
# TASK 1 UPDATE ORDERS_TABLE
ALTER TABLE orders_table
ALTER COLUMN date_uuid SET DATA TYPE UUID USING date_uuid::UUID,
ALTER COLUMN user_uuid SET DATA TYPE UUID USING user_uuid::UUID,
ALTER COLUMN card_number TYPE VARCHAR(20),
ALTER COLUMN store_code TYPE VARCHAR(12),
ALTER COLUMN product_code TYPE VARCHAR(11),
ALTER COLUMN product_quantity TYPE SMALLINT; 

# TASK 2 UPDATE DIM_USERS
ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255),
ALTER COLUMN last_name TYPE VARCHAR(255),
ALTER COLUMN date_of_birth TYPE DATE,
ALTER COLUMN country_code TYPE VARCHAR(2),
ALTER COLUMN user_uuid SET DATA TYPE UUID USING user_uuid::UUID,
ALTER COLUMN join_date TYPE DATE; 

# TASK 3 UPDATE DIM_STORE_DETAILS
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

# TASK 4 MAKE CHANGES TO DIM_PRODUCTS - Is there a smarter way of updating /creating this column
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

# TASK 5 UPDATE DIM_PRODUCTS
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

# TASK 6 UPDATE DIM_DATE_TIMES
ALTER TABLE dim_date_times
ALTER COLUMN month TYPE CHAR(2),
ALTER COLUMN year TYPE CHAR(4),
ALTER COLUMN day TYPE CHAR(2),
ALTER COLUMN time_period TYPE VARCHAR(255),
ALTER COLUMN date_uuid SET DATA TYPE UUID USING date_uuid::UUID; 

# TASK 7 UPDATE DIM_CARD_DETAILS
ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(20),
ALTER COLUMN expiry_date TYPE VARCHAR(10),
ALTER COLUMN date_payment_confirmed TYPE DATE; 

# TASK 8 SET PRIMARY KEYS
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

# TASK 9 SET FOREIGN KEYS - currently most of them not working, could be down to overcleaned data
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


# MILESTONE 4
# TASK 1
SELECT country_code, COUNT(*) FROM dim_store_details
GROUP BY country_code; 
# RETURNS: GB 256 (should be 265), DE 136 (should be 141), US 33 (should be 34)

# TASK 2
SELECT locality, COUNT(*) FROM dim_store_details
GROUP BY 1
ORDER BY 2 DESC;
# RETURNS Chapletown 14, Belper 13, Exeter 11, Buchley 10 (should be 12), High Wycombe 10, Surbiton 9 (?) other values deleted

# TASK 3
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
# results returned vary slightly from the predicted output - investigate, also round them

# TASK 4
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

# TASK 5
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
# results are very close, but not exact and the numbers are not rounded to 2 decimal places

# TASK 6
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
# the results are similar, but again a bit off (here it affects the years and months significantly, even though the total sales are not far off)

# Alternatively:
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

# TASK 7
SELECT SUM(staff_numbers), country_code
FROM dim_store_details
GROUP BY 2
ORDER BY 1 DESC;
# as above, results are pretty close, but not exactly the same
# NOTE could replace country_code for Web with Web

# TASK 8 
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
# order of results the same, but values completely different, they might be after number of transactions rather than actual total sales
# the below returns more expected results, but once again the values are different, worth investigating if your code is not over-cleaning
SELECT COUNT(*) AS total_sales,
dim_store_details.store_type,
dim_store_details.country_code
FROM orders_table
JOIN dim_products ON orders_table.product_code = dim_products.product_code
JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY 2, 3
HAVING country_code = 'DE'
ORDER BY 1; 

# TASK 9
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
# relatively similar results, but some differences, review cleaning methods









