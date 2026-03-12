use pizza;

use schema public;
use schema staging;
use schema mart ;

show schemas;

show tables ;

drop table if exists  pizza.mart.fact_sales;


select * from dim_date;
select * from dim_pizza;
select * from fact_table;

describe table pizza.mart.fact_table;

-- I will Begin EDA now using sql 

-- first i will count rows basically doing sanity check to see all rows i have or not , nulls values as well as duplicates then i will begin the EDA

select count(*) from pizza.mart.fact_table;

select count(*) from pizza.staging.order_details_raw_stg;


--  checking nulls in fact table 

select count(*) as total_rows,
sum(case when sales_key is null then 1 else 0 end ) as sales_key_null,
sum(case when date_key is null then 1 else 0 end ) as date_key_null,
sum(case when pizza_key is null then 1 else 0 end ) as pizza_key_null,
sum(case when order_id is null then 1 else 0 end ) as order_id_null,
sum(case when quantity is null then 1 else 0 end ) as quantity_null,
sum(case when price is null then 1 else 0 end ) as price_null,
sum(case when revenue is null then 1 else 0 end ) as revenue_null,

from pizza.mart.fact_table;


-- cheking if any numerical column is less that 0 or negative 

select count(*) as total_rows,
sum(case when quantity <= 0 then 1 else 0 end) as chk_quan,
sum(case when price <= 0 then 1 else 0 end) as chk_price,
sum(case when revenue <= 0 then 1 else 0 end) as chk_revenue,
from pizza.mart.fact_table;

-- now i will check if the revenue column is fine or not 

select count(*) as mismatch_col
from pizza.mart.fact_table
where revenue <> quantity * price
or revenue is null 
or quantity is null 
or price is null 

--  checking the refrential integrity means every foreign key has refrence to primary keys in dim_tables.

SELECT COUNT(*) AS missing_dates
FROM pizza.MART.FACT_TABLE f
LEFT JOIN pizza.MART.DIM_DATE d
    ON f.date_key = d.date_key
WHERE d.date_key IS NULL;


SELECT COUNT(*) AS missing_pizzas
FROM pizza.MART.FACT_TABLE f
LEFT JOIN pizza.MART.DIM_PIZZA p
    ON f.pizza_key = p.pizza_key
WHERE p.pizza_key IS NULL;



--  will start with the eda now as all the data quality check is done and everything seems fine so.

--  total revenue 

select sum(revenue) as Total_Revenue,
sum(quantity) as Total_Pizza_Sold ,
count(distinct(order_id)) as Total_Orders,
round(sum(quantity)/count(distinct order_id),0) as Average_pizzas_per_order,
round(sum(revenue) / count(distinct order_id),2) as Average_Price_per_order,
round(sum(revenue) / sum(quantity),2) as Average_pizza_price
from pizza.mart.fact_table



--  doing time based analysis phase -2

-- which month has most revenue ?
select month_name ,round(sum(revenue),1) as Total_Revenue 
from pizza.mart.fact_table fact
inner join pizza.mart.dim_date date_ on fact.date_key = date_.date_key
group by month_name
order by Total_Revenue desc 

-- which quarter made most revenue ?

select quarter ,round(sum(revenue),1) as Total_Revenue 
from pizza.mart.fact_table fact
inner join pizza.mart.dim_date date_ on fact.date_key = date_.date_key
group by quarter
order by Total_Revenue desc 

--  weekday , weeknd which made most revenue

select is_weekend ,round(sum(revenue),1) as Total_Revenue 
from pizza.mart.fact_table fact
inner join pizza.mart.dim_date date_ on fact.date_key = date_.date_key
group by is_weekend
order by Total_Revenue desc 

-- how much pizzas sold by hours and day 

select day_name, hour(cast(full_time as time)) as hours, count(order_id) as total_orders, round(sum(revenue),1) as total_revenue
from pizza.mart.fact_table fact 
inner join pizza.mart.dim_date date_ on fact.date_key = date_.date_key
group by hours,day_name
order by total_orders desc
limit 10


-- month over month analysis 
with monthly as (
    select month, month_name, round(sum(revenue),1) as monthly_revenue
    from pizza.mart.fact_table f
    inner join pizza.mart.dim_date d on f.date_key = d.date_key
    group by month, month_name
)
select month_name, monthly_revenue,
    lag(monthly_revenue) over(order by month) as prev_month,
    round((monthly_revenue - lag(monthly_revenue) over(order by month))
        / lag(monthly_revenue) over(order by month) * 100, 2) as mom_growth_pct
from monthly
order by month

-- which day most revenue made
select day_name ,round(sum(revenue),1) as Total_Revenue 
from pizza.mart.fact_table fact
inner join pizza.mart.dim_date date_ on fact.date_key = date_.date_key
group by day_name
order by Total_Revenue desc 

-- how many pizza sold on days 
select day_name ,count(distinct order_id) as total_order 
from pizza.mart.fact_table fact
inner join pizza.mart.dim_date date_ on fact.date_key = date_.date_key
group by day_name
order by total_order desc 

--  which day most money spent 
select day_name, ROUND(SUM(revenue) / COUNT(DISTINCT order_id), 2) as avg_day_spend 
from pizza.mart.fact_table fact
inner join pizza.mart.dim_date date_ on fact.date_key = date_.date_key
group by day_name
order by avg_day_spend desc 


-- phase 3

--  which category makes more profit 

select category , round(sum(revenue),1) as total_Sales_each_cat,count(distinct order_id) as total_orders
from pizza.mart.dim_pizza p
inner join pizza.mart.fact_table f on p.pizza_key = f.pizza_key
group by category
order by total_Sales_each_cat desc

-- total revenue by size

select size , round(sum(revenue),1) as total_Sales_each_size,count(distinct order_id) as total_orders
from pizza.mart.dim_pizza p
inner join pizza.mart.fact_table f on p.pizza_key = f.pizza_key
group by size
order by total_Sales_each_size desc

-- total revenue by pizza name

select pizza_name , round(sum(revenue),1) as total_Sales_each_p_name,count(distinct order_id) as total_orders
from pizza.mart.dim_pizza p
inner join pizza.mart.fact_table f on p.pizza_key = f.pizza_key
group by pizza_name
order by total_Sales_each_p_name desc

-- which pizza sold most orders 

select pizza_name , count(distinct order_id) as total_orders
from pizza.mart.dim_pizza p
inner join pizza.mart.fact_table f on p.pizza_key = f.pizza_key
group by pizza_name
order by total_orders desc

-- top 5 and bottom 5 pizzas by revenue

(select pizza_name, round(sum(revenue),1) as total_revenue, 'Top 5' as rank_group
from pizza.mart.dim_pizza p
inner join pizza.mart.fact_table f on p.pizza_key = f.pizza_key
group by pizza_name order by total_revenue desc limit 5)
UNION ALL
(select pizza_name, round(sum(revenue),1) as total_revenue, 'Bottom 5' as rank_group
from pizza.mart.dim_pizza p
inner join pizza.mart.fact_table f on p.pizza_key = f.pizza_key
group by pizza_name order by total_revenue asc limit 5)

-- calculating categpry + size wise total_Revenue and total_qty

select category, size, round(sum(revenue),1) as total_revenue,
sum(quantity) as total_qty
from pizza.mart.dim_pizza p
inner join pizza.mart.fact_table f on p.pizza_key = f.pizza_key
group by category, size
order by category, total_revenue desc


-- pareto analysis 

WITH pizza_revenue AS (
    SELECT 
        pizza_name,
        ROUND(SUM(revenue), 1) AS total_revenue,
        ROUND(SUM(revenue) / SUM(SUM(revenue)) OVER() * 100, 2) AS revenue_pct
    FROM pizza.mart.dim_pizza p
    INNER JOIN pizza.mart.fact_table f ON p.pizza_key = f.pizza_key
    GROUP BY pizza_name
)
SELECT 
    pizza_name,
    total_revenue,
    revenue_pct,
    ROUND(SUM(revenue_pct) OVER(ORDER BY total_revenue DESC), 2) AS cumulative_pct
FROM pizza_revenue
ORDER BY total_revenue DESC;



 --  full 
alter table pizza.mart.fact_table
add column full_time varchar

update pizza.mart.fact_table f
set full_time = d.full_time
from pizza.mart.dim_date d
where f.date_key = d.date_key;


select full_time from pizza.mart.fact_table limit 5



alter table pizza.mart.dim_date
drop column full_time;


select count(*), count(distinct date_key)
from pizza.mart.dim_date

create or replace table pizza.mart.dim_date as
select distinct
    date_key,
    full_date,
    year,
    month,
    month_name,
    day,
    day_name,
    quarter,
    is_weekend
from pizza.mart.dim_date;




