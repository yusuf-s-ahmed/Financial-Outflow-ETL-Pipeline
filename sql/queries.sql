-- Create a warehouse
CREATE WAREHOUSE data_warehouse_2
  WITH WAREHOUSE_SIZE = 'XSMALL'      -- size options: XSMALL, SMALL, MEDIUM, etc.
  WAREHOUSE_TYPE = 'STANDARD'         -- or 'SNOWPARK-OPTIMIZED'
  AUTO_SUSPEND = 300                  -- auto suspend after 5 minutes of inactivity
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- Create a database
CREATE DATABASE pre_operating_expenses_sample;

-- Create a schema inside the database
CREATE SCHEMA expense_data_sample;  -- or name it something meaningful to your project


-- Switch to the newly created database
USE DATABASE pre_operating_expenses_sample;


USE SCHEMA expense_data_sample;

show tables;

select * from categories;
select * from expenses;
select * from vendors;

--------------- Full table view -----------------

select 
    e.expense_id,
    e.date_of_expense,
    e.cost,
    c.category_type,
    c.expense_name,
    v.vendor_name,
    v.vendor_contact_information as contact_info
from 
    expenses e 
join categories c on e.category_id = c.category_id
join vendors v on e.vendor_id = v.vendor_id;


-------- 1. Total expenses per cateogry ------------

select 
    sum(e.cost) as TOTAL_AMOUNT,
    c.category_type
from
    expenses e
join categories c on e.category_id = c.category_id
group by c.category_type;

----- 2. Total expense per category per month ------

select 
    monthname(e.date_of_expense) as MONTH,
    year(e.date_of_expense) as YEAR,
    sum(e.cost) as TOTAL_AMOUNT,
    c.category_type
from
    expenses e
join categories c on e.category_id = c.category_id
group by c.category_type, monthname(e.date_of_expense), year(e.date_of_expense); 


--------- 3. Historical expenses time series -----------

select 
    e.cost,
    e.date_of_expense
from 
    expenses e;

----- 4. Category with highest total cost per month at N years ------

with monthly_totals as (
    select 
        monthname(e.date_of_expense) as MONTH,
        year(e.date_of_expense) as YEAR,
        sum(e.cost) as TOTAL_AMOUNT,
        c.category_type
    from
        expenses e
    join categories c on e.category_id = c.category_id
    group by c.category_type, monthname(e.date_of_expense), year(e.date_of_expense)
),
ranked_1 as (
    select 
        month,
        year,
        category_type,
        total_amount,
        row_number() over (
            partition by year, month
            order by total_amount desc
        ) as rn_1
    from monthly_totals
    where year = 2025
)

select
    month,
    year,
    total_amount,
    category_type
from ranked_1
where rn_1 = 1
order by year, month;



------ 5. Category with highest total cost per year -------


with year_totals as (
    select 
        year(e.date_of_expense) as YEAR,
        sum(e.cost) as TOTAL_AMOUNT,
        c.category_type
    from 
        expenses e
    join categories c on e.category_id = c.category_id
    group by c.category_type, year(e.date_of_expense)
),
ranked_2 as (
    select 
        year,
        category_type,
        total_amount,
        row_number() over (
            partition by year
            order by total_amount desc
        ) as rn_2
    from year_totals
)

select
    year,
    total_amount,
    category_type
from ranked_2
where rn_2 = 1
order by year;