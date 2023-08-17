# SQL-Data-Analysis-for-application-users

#SQL Query 1:

with p_users as
(
select user_name,
[lifecycle_stage],
[date],
min(date) over(partition by user_name order by date asc) as min_date,
max(date) over(partition by user_name order by date asc) as max_date,
DENSE_RANK() over(partition by user_name order by date desc) as rnk
from [dbo].['Packt_users'])
-- Created temporary table as p_users to create the necessary window of date
to further calculate the date difference.
select user_name,
lifecycle_stage as current_lifecycle_stage,
coalesce(datediff("DAY", min_date, max_date),0) as duration_in_days
from p_users
where rnk = 1;



#SQL Query 2:

With P_total_sales as
(select distinct * from [dbo].['Packt_sales']),
p_sales as
(select
[ID (int)],
[SaleDate (date)],
[SaleAmount (Float)],
sum([SaleAmount (Float)]) over(partition by [SaleDate (date)] order by
[SaleDate (date)]) as Daily_sales,
row_number() over (partition by [SaleDate (date)] order by [SaleAmount (Float)]
desc) as rnk
from P_total_sales),
p_sales_new as
(select [ID (int)],
[SaleDate (date)],
daily_sales,
coalesce(lag(daily_sales) over(order by [SaleDate (date)]),0) as Prev_day_sales
from p_sales
where rnk = 1)
select *
/*, case
when Prev_day_sales > Daily_sales then 'Lower sales than prev day sales'
when Prev_day_sales < Daily_sales then 'Higher sales than prev day sales'
else 'Equal sales than prev day sales' end as Daily_sales_status */
from p_sales_new
where Daily_sales > Prev_day_sales;



#SQL Query 3:

with p_contact as
(select *,
ROW_NUMBER() over(partition by [ID (int)] order by [ID (int)]) as rnk
from [dbo].['Packt_contact'])
Delete from p_contact
where rnk = 2;
select [ID (int)],
[Email (string)] as Invalid_emails
from [dbo].['Packt_contact']
where [Email (string)] not like '%_@%_.com';



#SQL Query 4:
With p_activity as
(select *,
lag([activity_week]) over(partition by user_id order by [activity_week]) as
prev_activity_week ,
dense_rank() over(partition by user_id order by activity_week) as rnk
from [dbo].['Packt_activity']),
P_activity_1 as
(select *,
coalesce(DATEDIFF("DAY", prev_activity_week, activity_week),0) as
day_count_between_act_week
from p_activity),
packt_user_activity as
(select *,
case
when max(day_count_between_act_week) over (partition by user_id order by
[activity_week] ) <= 7
then dense_rank() over(partition by user_id order by activity_week)
else 1
end as streak_number
from P_activity_1
where rnk < 5)
select
user_id,
activity_date,
activity_week,
streak_number,
case
when streak_number % 4 = 0 then 1
else 0 end as credited
from packt_user_activity;

