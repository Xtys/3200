--Task 2
DROP VIEW IF EXISTS profit_quarter_2021;
DROP VIEW IF EXISTS profit_year;
DROP MATERIALIZED VIEW IF EXISTS Sales_Time_Staff;


DROP VIEW IF EXISTS top3_profit;
DROP VIEW IF EXISTS most_profitable_item_ps;
DROP MATERIALIZED VIEW IF EXISTS Sales_Product_Staff;

--2.1 temp table
DROP TABLE IF EXISTS sale_temp CASCADE;

CREATE TABLE sale_temp(
    tid INT,
    sid INT,
    fname VARCHAR(20),
    lname VARCHAR(20),
    state VARCHAR(10),
    store VARCHAR(20),
    date DATE,
    pid INT,
    product VARCHAR(40),
    brand VARCHAR(40),
    unit_cost DECIMAL(10,2),
    quantity INT,
    price DECIMAL(10,2)
);

\copy sale_temp FROM '/home/s4775476/Sales.csv' DELIMITER ',' CSV HEADER QUOTE '"'

-- create dimension tables
DROP TABLE IF EXISTS Staff CASCADE;

CREATE TABLE Staff (
    sid INT PRIMARY KEY,
    fname VARCHAR(20),
    lname VARCHAR(20),
    store VARCHAR(20),
    state VARCHAR(10)
);

DROP TABLE IF EXISTS Product CASCADE;

CREATE TABLE Product (
    pid INT PRIMARY KEY,
    product VARCHAR(40),
    brand VARCHAR(40)
);

DROP TABLE IF EXISTS Time_Period CASCADE;

CREATE TABLE Time_Period (
    date DATE PRIMARY KEY,
    month VARCHAR(10),
    quarter VARCHAR(10),
    year INT
);

--Fact Table
DROP TABLE IF EXISTS Sales CASCADE;

CREATE TABLE Sales (
    tid INT,
    sid INT REFERENCES Staff(sid),
    pid INT REFERENCES Product(pid),
    date DATE REFERENCES Time_Period(date),
    quantity INT,
    price DECIMAL(10,2),
    unit_cost DECIMAL(10,2),
    PRIMARY KEY (tid, sid, pid, date)
);

--importing Data
INSERT INTO Staff (sid, fname, lname, store, state)
SELECT DISTINCT sid, fname, lname, store, state 
FROM sale_temp 
WHERE sid IS NOT NULL;

INSERT INTO Product (pid, product, brand)
SELECT DISTINCT pid, product, brand
FROM sale_temp
WHERE pid IS NOT NULL
ON CONFLICT (pid) DO NOTHING;

INSERT INTO Time_Period (date, month, quarter, year)
SELECT DISTINCT
    date,
    TRIM(TO_CHAR(date, 'Month')) AS month, 
    'Q' || EXTRACT(QUARTER FROM date) AS quarter,
    EXTRACT(YEAR FROM date) AS year
FROM sale_temp
WHERE date IS NOT NULL;

-- Populate Fact Table
INSERT INTO Sales (tid, sid, pid, date, quantity, price, unit_cost)
SELECT tid, sid, pid, date, quantity, price, unit_cost
FROM sale_temp
WHERE sid IS NOT NULL AND pid IS NOT NULL AND date IS NOT NULL;


-- Task 2.2
\echo '2.2 part a'
SELECT COUNT(*) AS unique_staff
FROM Staff;

\echo '2.2 part b'
SELECT COUNT(*) AS transaction_count
FROM Sales s
JOIN Time_Period t ON s.date = t.date
WHERE t.year = 2022 AND t.quarter = 'Q3';


-- Task 2.3

CREATE MATERIALIZED VIEW Sales_Time_Staff AS
SELECT 
    s.state, t.quarter, t.year, 
    SUM(f.quantity * (f.price - f.unit_cost)) AS total_profit
FROM Sales f
JOIN Staff s ON f.sid = s.sid
JOIN Time_Period t ON f.date = t.date
GROUP BY CUBE (s.state, t.quarter, t.year);


-- Task 2.4:
--quarter
CREATE VIEW profit_quarter_2021 AS
SELECT state, quarter, total_profit AS profit
FROM Sales_Time_Staff
WHERE year = 2021 AND STATE IS NOT NULL AND quarter is not null
ORDER BY state, quarter;


-- year
CREATE VIEW profit_year AS
SELECT state, year, total_profit AS profit
FROM Sales_Time_Staff
WHERE quarter is null And year is not null AND state is not null
ORDER BY state, year;

--query 2.4
SELECT * FROM profit_quarter_2021;
SELECT * FROM profit_year;

-- --Task 2.5
CREATE MATERIALIZED VIEW Sales_Product_Staff AS
SELECT s.store, p.product, p.brand,
    SUM(f.quantity * (f.price - f.unit_cost)) AS total_profit
FROM Sales f
JOIN Staff s ON f.sid = s.sid
JOIN Product p ON f.pid = p.pid
GROUP BY CUBE (s.store, p.product, p.brand);

--top 3 part a
CREATE VIEW top3_profit AS
SELECT store, total_profit AS gross_profit
FROM Sales_Product_Staff
WHERE store is not null AND product IS NULL AND brand is null
ORDER BY gross_profit DESC
LIMIT 3;

--Most profitable part b
CREATE VIEW most_profitable_item_ps AS
SELECT DISTINCT ON (store)store, brand, product, total_profit AS most_profit
FROM Sales_Product_Staff
WHERE brand is not null and store is not null and product is not null
ORDER BY store, most_profit DESC;

--query 2.5
SELECT * FROM top3_profit;
SELECT * FROM most_profitable_item_ps;