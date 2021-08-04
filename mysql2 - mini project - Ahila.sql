create database sql2_mini_project;
use sql2_mini_project;

#1.	Join all the tables and create a new table called combined_table.
# (market_fact, cust_dimen, orders_dimen, prod_dimen, shipping_dimen)
create table combined_table(
select m.*, c.customer_name,c.province,c.region,c.customer_segment,o.order_id,o.order_date,o.order_priority,
p.product_category,p.product_sub_category, s.ship_mode,s.ship_date
from market_fact m left join cust_dimen c
on m.cust_id = c.cust_id
left join orders_dimen o
on o.ord_id = m.ord_id
left join prod_dimen p 
on p.prod_id = m.prod_id
left join shipping_dimen s
on s.ship_id = m.ship_id);

select * from combined_table;

# 2.	Find the top 3 customers who have the maximum number of orders
with maximum  as(
select cust_id, count(ord_id), dense_rank() over(order by count(ord_id) desc) order_rank from combined_table group by cust_id)
select * from maximum where order_rank <=3;

# 3.	Create a new column DaysTakenForDelivery that contains the date difference of Order_Date and Ship_Date.

select combined_table.* , datediff(str_to_date(ship_date,'%m/%d/%Y'),str_to_date(order_date,'%d-%m-%Y')) days_taken_for_delivery from combined_table;
desc combined_table;
# 4.	Find the customer whose order took the maximum time to get delivered.

with time_maximum as
(select combined_table.* , datediff(str_to_date(ship_date,'%m/%d/%Y'),str_to_date(order_date,'%d-%m-%Y')) days_taken_for_delivery from combined_table)

select * from time_maximum order by days_taken_for_delivery desc limit 1 ;


# 5.	Retrieve total sales made by each product from the data (use Windows function)

select distinct product_sub_category, round(sum(sales) over(partition by product_sub_category),2) Total_sales from combined_table;


# 6.	Retrieve total profit made from each product from the data (use windows function)
	
select distinct product_sub_category, round(sum(profit) over(partition by product_sub_category),2) Total_profit from combined_table;

# 7.	Count the total number of unique customers in January and how many of them came back every month over the entire year in 2011

with unique_customers_jan11 as (select distinct cust_id from combined_table where month(order_date) = 1 and year(order_date) =2011)
select count(cust_id) from unique_customers_jan11;

select cust_id from combined_table where year(order_date)=2011 group by cust_id having count(distinct month(order_date))>11 ;


# 8.	Retrieve month-by-month customer retention rate since the start of the business.(using views)

# Tips: 
#1: Create a view where each user’s visits are logged by month, allowing for the possibility that these will have occurred over multiple # years since whenever business started operations
# 2: Identify the time lapse between each visit. So, for each person and for each month, we see when the next visit is.
# 3: Calculate the time gaps between visits
# 4: categorise the customer with time gap 1 as retained, >1 as irregular and NULL as churned
# 5: calculate the retention month wise

create or replace view retention_rate as (
select cust_id,order_date,  lead(order_date,1) over(partition by cust_id order by str_to_date(order_date,'%d-%m-%Y')) next_visit from combined_table); 

desc retention_rate;
select * from retention_rate;
select * , 
period_diff( concat (year(str_to_date(next_visit,'%d-%m-%Y')),date_format(str_to_date(next_visit,'%d-%m-%Y'),"%m")),
concat(year(str_to_date(order_date,'%d-%m-%Y')), date_format(str_to_date(order_date,'%d-%m-%Y'), "%m"))) time_gap from retention_rate;

create or replace view report as
(with comments as(
select * , 
period_diff( concat (year(str_to_date(next_visit,'%d-%m-%Y')),date_format(str_to_date(next_visit,'%d-%m-%Y'),"%m")),
concat(year(str_to_date(order_date,'%d-%m-%Y')), date_format(str_to_date(order_date,'%d-%m-%Y'), "%m"))) time_gap from retention_rate)


select *, 
case 
when time_gap =0 or time_gap=1 then "retained"
when time_gap >1 then "irregular"
when time_gap = null then "churned"
end comments
 from comments);
 
select * from report;
 create or replace view  retained as (
 select year(str_to_date(order_date,'%d-%m-%Y')) year, month(str_to_date(order_date,'%d-%m-%Y')) month,count(cust_id) retained from report where time_gap<=1 group by year(str_to_date(order_date,'%d-%m-%Y')),month(str_to_date(order_date,'%d-%m-%Y')) 
 order by year(str_to_date(order_date,'%d-%m-%Y')),month(str_to_date(order_date,'%d-%m-%Y')));
 select * from retained;
 create or replace view total_customer as(
 select year(str_to_date(order_date,'%d-%m-%Y')) year, month(str_to_date(order_date,'%d-%m-%Y')) month ,count(cust_id) total_customer from report  group by year(str_to_date(order_date,'%d-%m-%Y')),month(str_to_date(order_date,'%d-%m-%Y')) 
 order by year(str_to_date(order_date,'%d-%m-%Y')),month(str_to_date(order_date,'%d-%m-%Y')));
 
 select * from total_customer;
 select tc.* , r.retained,  (retained/total_customer)*100 , 
 avg( (retained/total_customer)*100 ) over()
 from total_customer tc left join retained r 
 on tc.year = r.year and tc.month =r.month;
 
 
