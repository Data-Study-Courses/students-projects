#Рассчетные столбцы в sql(можно скопировать весь текст сразу и выполнять ячейки подряд)
#Создание нового поля с количеством заказов пользователя


ALTER TABLE df_2017
ADD COLUMN quantity_orders INTEGER


#Обновляем данные столбца quantity_orders


UPDATE df_2017
SET quantity_orders = sub_query.total_orders
FROM (
    SELECT customer_id, COUNT(DISTINCT order_id) AS total_orders
    FROM df_2017
    GROUP BY customer_id
) AS sub_query
WHERE df_2017.customer_id = sub_query.customer_id;


#Создание новой таблицы с данными о клиентах для RFM анализа


CREATE TABLE rfm_2017 (
    customer_id TEXT,
    customer_name TEXT,
    orders_quantity INTEGER,
    orders_sum REAL,
    avg_order REAL,
    last_order TEXT,
    last_order_day INTEGER,
    rfm_recency INTEGER,
    rfm_frequency INTEGER,
    rfm_monetary INTEGER,
    rfm_cell INTEGER,
    rfm_cell_string TEXT
);


#Создание новых столбцов для рфм рассчетов


INSERT INTO rfm_2017 (customer_id, customer_name, orders_quantity, orders_sum, avg_order, last_order, last_order_day, rfm_recency, rfm_frequency, rfm_monetary, rfm_cell, rfm_cell_string)
WITH rfm_2017 AS (
    SELECT
        customer_id,
        customer_name,
        COUNT(DISTINCT order_id) AS orders_quantity,
        ROUND(SUM(sales), 2) AS orders_sum,
        ROUND(AVG(sales), 2) AS avg_order,
        MAX(order_date) AS last_order,
        julianday((SELECT MAX(order_date) FROM df_2017)) - julianday(MAX(order_date)) AS last_order_day
    FROM
        df_2017
    GROUP BY
        customer_id, customer_name
),
rfm_calc AS (
    SELECT
        r.*,
        NTILE(4) OVER (ORDER BY last_order_day ASC) AS rfm_recency,
        NTILE(4) OVER (ORDER BY orders_quantity ASC) AS rfm_frequency,
        NTILE(4) OVER (ORDER BY orders_sum ASC) AS rfm_monetary
    FROM
        rfm_2017 AS r
)
SELECT
    customer_id,
    customer_name,
    orders_quantity,
    orders_sum,
    avg_order,
    last_order,
    last_order_day,
    rfm_recency,
    rfm_frequency,
    rfm_monetary,
    rfm_recency + rfm_frequency + rfm_monetary AS rfm_cell,
    CONCAT(rfm_recency, rfm_frequency, rfm_monetary) AS rfm_cell_string
FROM
    rfm_calc;



# Создадим новую таблицу с всеми данными и разметкой клиентов по рфм сегментам


CREATE TABLE df_rfm_2017 AS
SELECT 
    *,
    CASE
    WHEN rfm_cell_string IN ('111', '112', '113', '114', '121', '122', '123', '124', '131', '132', '133', '134', '141', '142', '143', '144') THEN 'Новые клиенты'
    WHEN rfm_cell_string IN ('211', '212', '213', '214', '221', '222', '223', '224', '231', '232', '233', '234', '241', '242', '243', '244') THEN 'Лояльные клиенты'
    WHEN rfm_cell_string IN ('311', '312', '313', '314', '321', '322', '323', '324', '331', '332', '333', '334', '341', '342', '343', '344') THEN 'Спящие клиенты'
    WHEN rfm_cell_string IN ('411', '412', '413', '414', '421', '422', '423', '424', '431', '432', '433', '434', '441', '442', '443', '444') THEN 'Потерянные клиенты'
END AS rfm_segment
FROM 
    df_2017 df
JOIN 
    rfm_2017 AS rfm ON df.customer_id = rfm.customer_id;





#Метод присвоения чиcельного значение каждому сегменту основано на:

#Recency (R):
#Меньшее количество дней в R означает, что клиент делал покупку недавно и это хорошо (присваиваем класс 1).
#Большее количество дней в R означает, что клиент давно не делал покупок и это плохо (присваиваем класс 4).

#Frequency (F) и Monetary (M):
#Меньшие значения F и M означают, что клиент редко покупает и/или приносит низкий доход, что плохо (присваиваем класс 1).
#Большие значения F и M означают, что клиент часто покупает и/или приносит высокий доход, что хорошо (присваиваем класс 4).

#С уточнением сортировки по увеличению значений на этапе разбиения на квантили:
#        NTILE(4) OVER (ORDER BY last_order_day ASC) AS rfm_recency,
#        NTILE(4) OVER (ORDER BY orders_quantity ASC) AS rfm_frequency,
#        NTILE(4) OVER (ORDER BY orders_sum ASC) AS rfm_monetary




#            По итогу в СУБД должно отображаться 6 таблиц (не оптимально но диверсифицированно от изменений в тетрадке юпитера) 
#            Список таблиц df_2017 / df_2018 / rfm_2017 / rfm_2018 / df_rfm_2017 / df_rfm_2018
#            Из СУБД мы импортируем в ручную 2 последних в списке файла и строим дашборд в BI инструменте (на ваш выбор) в моем случае DATALENS



#Тот же код только для 2018 года


ALTER TABLE df_2018
ADD COLUMN quantity_orders INTEGER

UPDATE df_2018
SET quantity_orders = sub_query.total_orders
FROM (
    SELECT customer_id, COUNT(DISTINCT order_id) AS total_orders
    FROM df_2018
    GROUP BY customer_id
) AS sub_query
WHERE df_2018.customer_id = sub_query.customer_id;

CREATE TABLE rfm_2018 (
    customer_id TEXT,
    customer_name TEXT,
    orders_quantity INTEGER,
    orders_sum REAL,
    avg_order REAL,
    last_order TEXT,
    last_order_day INTEGER,
    rfm_recency INTEGER,
    rfm_frequency INTEGER,
    rfm_monetary INTEGER,
    rfm_cell INTEGER,
    rfm_cell_string TEXT
);


INSERT INTO rfm_2018 (customer_id, customer_name, orders_quantity, orders_sum, avg_order, last_order, last_order_day, rfm_recency, rfm_frequency, rfm_monetary, rfm_cell, rfm_cell_string)
WITH rfm_2018 AS (
    SELECT
        customer_id,
        customer_name,
        COUNT(DISTINCT order_id) AS orders_quantity,
        ROUND(SUM(sales), 2) AS orders_sum,
        ROUND(AVG(sales), 2) AS avg_order,
        MAX(order_date) AS last_order,
        julianday((SELECT MAX(order_date) FROM df_2018)) - julianday(MAX(order_date)) AS last_order_day
    FROM
        df_2018
    GROUP BY
        customer_id, customer_name
),
rfm_calc AS (
    SELECT
        r.*,
        NTILE(4) OVER (ORDER BY last_order_day ASC) AS rfm_recency,
        NTILE(4) OVER (ORDER BY orders_quantity ASC) AS rfm_frequency,
        NTILE(4) OVER (ORDER BY orders_sum ASC) AS rfm_monetary
    FROM
        rfm_2018 AS r
)
SELECT
    customer_id,
    customer_name,
    orders_quantity,
    orders_sum,
    avg_order,
    last_order,
    last_order_day,
    rfm_recency,
    rfm_frequency,
    rfm_monetary,
    rfm_recency + rfm_frequency + rfm_monetary AS rfm_cell,
    CONCAT(rfm_recency, rfm_frequency, rfm_monetary) AS rfm_cell_string
FROM
    rfm_calc;


CREATE TABLE df_rfm_2018 AS
SELECT 
    *,
    CASE
    WHEN rfm_cell_string IN ('111', '112', '113', '114', '121', '122', '123', '124', '131', '132', '133', '134', '141', '142', '143', '144') THEN 'Новые клиенты'
    WHEN rfm_cell_string IN ('211', '212', '213', '214', '221', '222', '223', '224', '231', '232', '233', '234', '241', '242', '243', '244') THEN 'Лояльные клиенты'
    WHEN rfm_cell_string IN ('311', '312', '313', '314', '321', '322', '323', '324', '331', '332', '333', '334', '341', '342', '343', '344') THEN 'Спящие клиенты'
    WHEN rfm_cell_string IN ('411', '412', '413', '414', '421', '422', '423', '424', '431', '432', '433', '434', '441', '442', '443', '444') THEN 'Потерянные клиенты'
END AS rfm_segment
FROM 
    df_2018 df
JOIN 
    rfm_2018 AS rfm ON df.customer_id = rfm.customer_id;