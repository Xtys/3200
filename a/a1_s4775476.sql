--Task 1
--1.1
SELECT COUNT(*)
FROM  employees;

--1.2
SELECT COUNT(DISTINCT emp_no)
FROM dept_emp
WHERE dept_no = (SELECT dept_no 
                FROM departments 
                WHERE dept_name = 'Marketing');

--Task 2
--2.1
CREATE TABLE IF NOT EXISTS salaries_horizontal
(
  emp_no int NOT NULL,
  salary int NOT NULL,
  from_date date NOT NULL,
  to_date date NOT NULL,
  PRIMARY KEY (emp_no, from_date),
  CONSTRAINT salaries_emp_no_fk FOREIGN KEY (emp_no) REFERENCES employees (emp_no)
) partition by range (from_date);

Create table IF NOT EXISTS salaries_h1 Partition of salaries_horizontal for values from (MINVALUE) to ('1990-01-01');
Create table IF NOT EXISTS salaries_h2 Partition of salaries_horizontal for values from ('1990-01-01') to ('1992-01-01');
Create table IF NOT EXISTS salaries_h3 Partition of salaries_horizontal for values from ('1992-01-01') to ('1994-01-01');
Create table IF NOT EXISTS salaries_h4 Partition of salaries_horizontal for values from ('1994-01-01') to ('1996-01-01');
Create table IF NOT EXISTS salaries_h5 Partition of salaries_horizontal for values from ('1996-01-01') to ('1998-01-01');
Create table IF NOT EXISTS salaries_h6 Partition of salaries_horizontal for values from ('1998-01-01') to ('2000-01-01');
Create table IF NOT EXISTS salaries_h7 Partition of salaries_horizontal for values from ('2000-01-01') to (MAXVALUE);

TRUNCATE TABLE salaries_horizontal;
INSERT INTO salaries_horizontal
SELECT *
FROM salaries;

--2.2
SELECT AVG(salary) AS average_salary
FROM salaries_horizontal
WHERE from_date >= '1996-06-30' AND from_date <= '1996-12-31';

EXPLAIN SELECT AVG(salary) AS average_salary
FROM salaries_horizontal
WHERE from_date >= '1996-06-30' AND from_date <= '1996-12-31';

--2.3 verticle fragmentation

DROP TABLE IF EXISTS employees_public;

create table employees_public(emp_no integer NOT NULL,
    first_name varchar(50) NOT NULL,
    last_name varchar(50) NOT NULL,
    hire_date date NOT NULL,
    PRIMARY KEY (emp_no));

INSERT INTO employees_public (emp_no, first_name, last_name, hire_date)
SELECT emp_no, first_name, last_name, hire_date
FROM employees;

--2.3 employees_confidential
--make DB 
CREATE DATABASE emp_confidential;

-- Generate SQL file
COPY (
    SELECT 
        'INSERT INTO employees_confidential (emp_no, birth_date, gender) VALUES (' || 
        emp_no || ', ''' || birth_date || ''', ''' || gender || ''');'
    FROM employees
) TO '/tmp/employees_confidential_data.sql' WITH (FORMAT TEXT);

-- Switch to new db
\c emp_confidential


DROP TABLE IF EXISTS employees_confidential;

create table employees_confidential(emp_no integer NOT NULL,
    birth_date date NOT NULL,
    gender char(1) NOT NULL,
    PRIMARY KEY (emp_no)
    );

-- Import SQL file
CREATE TEMPORARY TABLE temp_sql_statements (statement text);

COPY temp_sql_statements (statement)
FROM '/tmp/employees_confidential_data.sql' WITH (FORMAT TEXT);

DO $$
DECLARE
    sql_stmt text;
BEGIN
    FOR sql_stmt IN (SELECT statement FROM temp_sql_statements)
    LOOP
        EXECUTE sql_stmt;
    END LOOP;
END $$;


\c emp_s4775476


--task 4
-- 4.1: Establish FDW to sharedb

Create extension  IF NOT EXISTS postgres_fdw;

Create server sharedb_server
    Foreign data wrapper postgres_fdw
    OPTIONS (host 'infs3200-sharedb.zones.eait.uq.edu.au', port '5432', dbname 'sharedb');

--user mapping for the sharedb user
create user mapping for s4775476
    server sharedb_server
    OPTIONS (user 'sharedb', password 'Y3Y7FdqDSM9.3d47XUWg');

-- Create a foreign table to map the titles table
Create foreign table titles_f (
    emp_no integer NOT NULL,
    title varchar(50) NOT NULL,
    from_date date NOT NULL,
    to_date date
)
    server sharedb_server
    OPTIONS (schema_name 'public', table_name 'titles');

select count(*) from titles_f;

-- 4.2 Calculate average current salary per unique title

SELECT 
    t.title,
    AVG(s.salary) AS avg_current_salary
FROM public.titles_f t
JOIN (
    SELECT emp_no, salary, from_date
    FROM salaries s1
    WHERE from_date = (
        SELECT MAX(from_date)
        FROM salaries s2
        WHERE s2.emp_no = s1.emp_no
    )
) s
ON t.emp_no = s.emp_no
WHERE t.to_date = '9999-01-01'
GROUP BY t.title
ORDER BY avg_current_salary DESC;



--4.3 Establish FDW to emp_confidential

create server if not EXISTS emp_confidential_server
    Foreign data wrapper postgres_fdw
    OPTIONS (host 'localhost', port '5432', dbname 'emp_confidential');


create user mapping for s4775476
    SERVER emp_confidential_server
    OPTIONS (user 'readonly_user', password 'infs3200');

create foreign table IF NOT EXISTS employees_confidential_f (
    emp_no integer NOT NULL,
    birth_date date NOT NULL,
    gender char(1) NOT NULL)
    server emp_confidential_server options (schema_name 'public', table_name 'employees_confidential');

--grant access
\c emp_confidential
CREATE ROLE readonly_user WITH LOGIN PASSWORD 'infs3200';
GRANT CONNECT ON DATABASE emp_confidential TO readonly_user;
GRANT USAGE ON SCHEMA public TO readonly_user;
GRANT SELECT ON TABLE public.employees_confidential TO readonly_user;

\c emp_s4775476

WITH emp_no_modified AS (
    SELECT emp_no
    FROM employees_confidential_f
    WHERE birth_date >= '1970-01-01' AND birth_date < '1975-01-01'
)


-- last_name and first_name from employees_public
SELECT 
    ep.first_name,
    ep.last_name
FROM employees_public ep
WHERE ep.emp_no IN (SELECT emp_no FROM emp_no_modified)
ORDER BY ep.last_name, ep.first_name;


--4.4
-- Semi-join 
EXPLAIN
SELECT ep.first_name, ep.last_name, fr.birth_date
FROM employees_public ep,
    (SELECT ec.emp_no, ec.birth_date
     FROM employees_confidential_f ec, (SELECT emp_no FROM employees_public) ep
     WHERE ec.emp_no = ep.emp_no
     AND ec.birth_date >= '1970-01-01' AND ec.birth_date < '1975-01-01') fr
WHERE ep.emp_no = fr.emp_no;

-- Inner join
EXPLAIN
SELECT ep.first_name, ep.last_name, ec.birth_date, ec.gender
FROM employees_public ep
INNER JOIN employees_confidential_f ec
ON ep.emp_no = ec.emp_no
WHERE ep.emp_no IN (
    SELECT ec2.emp_no
    FROM employees_confidential_f ec2
    WHERE ec2.birth_date >= '1970-01-01' AND ec2.birth_date < '1975-01-01'
);