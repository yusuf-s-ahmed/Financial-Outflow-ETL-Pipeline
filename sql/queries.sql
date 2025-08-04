-- Create a warehouse
CREATE WAREHOUSE data_warehouse_2
  WITH WAREHOUSE_SIZE = 'XSMALL'      -- size options: XSMALL, SMALL, MEDIUM, etc.
  WAREHOUSE_TYPE = 'STANDARD'         -- or 'SNOWPARK-OPTIMIZED'
  AUTO_SUSPEND = 300                  -- auto suspend after 5 minutes of inactivity
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- Create a database
CREATE DATABASE pre_operating_expenses_sample;

-- Switch to the newly created database
USE DATABASE pre_operating_expenses_sample;

-- Create a schema inside the database
CREATE SCHEMA expense_data_sample;  -- or name it something meaningful to your project

show tables;

select * from categories;
select * from expenses;
select * from vendors;

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
    
    

