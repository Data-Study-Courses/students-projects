--ОБЪЕМ ПРОДАЖ
-- объем продаж +  процентное соотношение по штатам

select sm.state, 
round(sum(sm.sales::numeric)) as amount, 
round(sum(sum(sm.sales::numeric)) over ()) as total_amount,
concat(round(sum(sm.sales::numeric) / sum(sum(sm.sales::numeric)) over () * 100, 1), '%') as state_prc
from superstore_mart sm
group by sm.state
order by amount desc;

-- объем продаж +  процентное соотношение по сегментам

select sm.segment, 
round(sum(sm.sales::numeric)) as amount, 
round(sum(sum(sm.sales::numeric)) over()) as total_amount,
concat(round(sum(sm.sales::numeric) / sum(sum(sm.sales::numeric)) over () * 100, 1), '%') as segment_prc
from superstore_mart sm 
group by sm.segment
order by amount desc;

-- объем продаж +  процентное соотношение по категориям

select sm.category, 
round(sum(sm.sales::numeric)) as amount, 
round(sum(sum(sm.sales::numeric)) over()) as total_amount,
concat(round(sum(sm.sales::numeric) / sum(sum(sm.sales::numeric)) over () * 100, 1), '%') as category_prc
from superstore_mart sm 
group by sm.category
order by amount desc;

-- объем продаж +  процентное соотношение по подкатегориям

select sm.sub_category, 
round(sum(sm.sales::numeric)) as amount, 
round(sum(sum(sm.sales::numeric)) over()) as total_amount,
concat(round(sum(sm.sales::numeric) / sum(sum(sm.sales::numeric)) over () * 100, 1), '%') as sub_category_prc
from superstore_mart sm 
group by sm.sub_category
order by amount desc

-- объем продаж +  процентное соотношение по продуктам

select sm.product_name, 
round(sum(sm.sales::numeric)) as amount, 
round(sum(sum(sm.sales::numeric)) over()) as total_amount,
concat(round(sum(sm.sales::numeric) / sum(sum(sm.sales::numeric)) over() * 100, 1), '%') as product_prc
from superstore_mart sm 
group by sm.product_name
order by amount desc

--ТЕМП РОСТА ПРОДАЖ: ПРОЦЕНТНОЕ ИЗМЕНЕНИЕ ОБЪЕМА ПРОДАЖ ЗА ПЕРИОД ПО СРАВНЕНИЮ С ПРЕДЫДУЩИМ

--темп роста продаж: по годам

select extract(year from sm.order_date) as order_year, 
round(sum(sm.sales::numeric)) as year_amount,
case
	when lag(sum(sm.sales::numeric)) over (order by extract(year from sm.order_date)) is null then '-'
	else concat(round((sum(sm.sales::numeric) - lag(sum(sm.sales::numeric)) over (order by extract(year from sm.order_date))) / 
	lag(sum(sm.sales::numeric)) over (order by extract(year from sm.order_date)) * 100, 1), '%')
end as year_growth_rate
from superstore_mart sm 
group by extract(year from sm.order_date)
order by order_year

--темп роста продаж: по кварталам

select extract(year from sm.order_date) as order_year,
trim(to_char(sm.order_date, '"Q"Q')) as order_quart,
round(sum(sm.sales::numeric)) as quart_amount,
case
	when lag(sum(sm.sales::numeric)) over (order by extract(year from sm.order_date), to_char(sm.order_date, '"Q"Q')) is null then '-'
	else concat(round((sum(sm.sales::numeric) - 
	lag(sum(sm.sales::numeric)) over (order by extract(year from sm.order_date), to_char(sm.order_date, '"Q"Q'))) / 
	lag(sum(sm.sales::numeric)) over (order by extract(year from sm.order_date), to_char(sm.order_date, '"Q"Q')) * 100, 1), '%')
end as month_growth_rate
from superstore_mart sm 
group by extract(year from sm.order_date), to_char(sm.order_date, '"Q"Q')
order by order_year, order_quart

--темп роста продаж: по месяцам

select extract(year from sm.order_date) as order_year, 
trim(to_char(sm.order_date, '"Q"Q')) as order_quart,
concat(extract(month from sm.order_date)::text, '-', trim(to_char(sm.order_date, 'month'))) as order_month, 
round(sum(sm.sales::numeric)) as month_amount,
case
	when lag(sum(sm.sales::numeric)) over (order by extract (year from sm.order_date), extract(month from sm.order_date)) is null then '-'
	else concat(round((sum(sm.sales::numeric) - 
	lag(sum(sm.sales::numeric)) over (order by extract (year from sm.order_date), extract(month from sm.order_date))) / 
	lag(sum(sm.sales::numeric)) over (order by extract (year from sm.order_date), extract(month from sm.order_date)) * 100, 1), '%')
end as month_growth_rate
from superstore_mart sm 
group by extract(year from sm.order_date), extract(month from sm.order_date), to_char(sm.order_date, '"Q"Q'), to_char(sm.order_date, 'month')
order by order_year, order_quart, order_month

--ДОЛЯ КАЖДОЙ КАТЕГОРИИ В ОБЩЕМ ОБЪЕМЕ ПРОДАЖ

select sm.category, round(sum(sm.sales::numeric)) as amount,
concat(round(sum(sm.sales::numeric) / sum(sum(sm.sales::numeric)) over () * 100, 1), '%') as category_prc
from superstore_mart sm 
group by sm.category
order by amount desc

--ДОЛЯ КЛИЕНТОВ ПО КАТЕГОРИЯМ В ОБЩЕМ ОБЪЕМЕ ПРОДАЖ

select sm.segment, round(sum(sm.sales::numeric)) as amount,
concat(round(sum(sm.sales::numeric) / sum(sum(sm.sales::numeric)) over () * 100, 1), '%') as segment_prc
from superstore_mart sm 
group by sm.segment
order by amount desc

--ВРЕМЯ ДОСТАВКИ: среднее время от оформления заказа до его получения клиентом

with cte1 as(
select sm.ship_mode, avg(sm.ship_date - sm.order_date) as avg_del, count(*) as total_deliveries
from superstore_mart sm 
group by sm.ship_mode)
select sm.ship_mode, concat(round(cte1.avg_del), ' days') as avg_delivery_days,
sum(case
		when (sm.ship_date - sm.order_date) > cte1.avg_del then 1
		else 0
	end) as days_above_avg,
concat(round(100.0 * sum(case when (sm.ship_date - sm.order_date) > cte1.avg_del then 1 else 0 end) / cte1.total_deliveries, 2), '%') as prc_above_avg,
cte1.total_deliveries
from cte1
left join superstore_mart sm on cte1.ship_mode = sm.ship_mode
group by sm.ship_mode, cte1.avg_del, cte1.total_deliveries
order by avg_delivery_days

--RFM-АНАЛИЗ (последняя дата: 2019-01-01 для длительности с последней покупки)

with rfm as(
select sm.customer_id, sm.customer_name, 
(select '2019-01-01'::date) - max(sm.order_date) as recency, 
count(*) as frequency, 
sum(sm.sales::numeric) as monetary
from superstore_mart sm 
group by sm.customer_id, sm.customer_name),
percentiles as(
select percentile_cont(array[0.2, 0.4, 0.6, 0.8]) within group (order by recency) as recency_perc,
percentile_cont(array[0.2, 0.4, 0.6, 0.8]) within group (order by frequency) as frequency_perc,
percentile_cont(array[0.2, 0.4, 0.6, 0.8]) within group (order by monetary) as monetary_perc
from rfm),
rfm_score as(
select r.customer_id, r.customer_name, r.recency, r.frequency, r.monetary,
case
	when r.recency <= p.recency_perc[1] then 5
	when r.recency <= p.recency_perc[2] then 4
	when r.recency <= p.recency_perc[3] then 3
	when r.recency <= p.recency_perc[4] then 2
	else 1
end as r_score,
case
	when r.frequency <= p.frequency_perc[1] then 1
   	when r.frequency <= p.frequency_perc[2] then 2
   	when r.frequency <= p.frequency_perc[3] then 3
   	when r.frequency <= p.frequency_perc[4] then 4
   	else 5
end as f_score,
case
    when r.monetary <= p.monetary_perc[1] then 1
    when r.monetary <= p.monetary_perc[2] then 2
    when r.monetary <= p.monetary_perc[3] then 3
    when r.monetary <= p.monetary_perc[4] then 4
    else 5
end as m_score
from rfm r
cross join percentiles p),
rfm_group as(
select *, (r_score::text || f_score::text || m_score::text) as rfm_segment,
	case 
		when r_score = 5 and f_score >= 4 and m_score >= 4 then 'champions'
	    when r_score >= 4 and f_score >= 3 then 'loyal'
	    when r_score = 5 and f_score = 1 then 'new customers'
	    when r_score <= 2 and f_score >= 4 then 'at risk'
	    when r_score <= 2 and f_score <= 2 then 'lost'
	    else 'other'
	end as rfm_group
from rfm_score)
select rfm_group, count(*)
from rfm_group
group by rfm_group