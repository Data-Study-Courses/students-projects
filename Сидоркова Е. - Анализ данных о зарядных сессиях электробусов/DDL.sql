--Добавляем в основную таблицу pk_id в качестве первичного ключа, т.к. ни одна из колонок не имеет уникальных значений.
--Переименовываем основную таблицу

ALTER TABLE new_project.df_merge ADD COLUMN pk_id SERIAL PRIMARY key;
ALTER TABLE new_project.df_merge RENAME TO session_list;
ALTER TABLE new_project.session_list RENAME COLUMN stop_session_cause TO cause_id;
ALTER TABLE new_project.session_list RENAME COLUMN electrobus_type TO electrobus_type_id;

--В таблице df_address_clean назначаем первичный ключ, переименовываем таблицы с адресами
ALTER TABLE new_project.df_address_clean ADD PRIMARY KEY (id);
ALTER TABLE new_project.df_address_clean RENAME TO station_address;
ALTER TABLE new_project.df_address_anon RENAME TO station_address_anon;
ALTER TABLE new_project.station_address RENAME COLUMN id TO address_id;

--Выведем уникальные коды причины остановки сессии
SELECT DISTINCT stop_session_cause
FROM new_project.session_list;

--Создадим новый справочник на основе этого запроса, назначим первичный ключ и наполним справочник 
--расшифровкой кодов остановки
CREATE TABLE new_project.stop_session_cause AS 
	(SELECT DISTINCT stop_session_cause
FROM new_project.session_list);
ALTER TABLE new_project.stop_session_cause RENAME COLUMN stop_session_cause TO cause_id;
ALTER TABLE new_project.stop_session_cause ADD PRIMARY KEY (cause_id);
ALTER TABLE new_project.stop_session_cause ADD COLUMN stop_session_cause text;

UPDATE new_project.stop_session_cause 
SET stop_session_cause = 'Остановка ТС'
WHERE cause_id = 501;

UPDATE new_project.stop_session_cause 
SET stop_session_cause = 'Остановка ЭЗС'
WHERE cause_id = 502;

UPDATE new_project.stop_session_cause 
SET stop_session_cause = 'Остановка удаленным оператором'
WHERE cause_id = 551;

UPDATE new_project.stop_session_cause 
SET stop_session_cause = 'Код остановки не идентифицирован'
WHERE cause_id = 0;

--Выведем уникальные типы электробусов
SELECT DISTINCT electrobus_type
FROM new_project.session_list;

--Создадим новый справочник на основе этого запроса, назначим первичный ключ и наполним справочник 
--расшифровкой типов электробусов и их характеристик
CREATE TABLE new_project.electrobus_type AS (SELECT DISTINCT electrobus_type
FROM new_project.session_list);
ALTER TABLE new_project.electrobus_type RENAME COLUMN electrobus_type TO electrobus_type_id;
ALTER TABLE new_project.electrobus_type ADD PRIMARY KEY (electrobus_type_id);
ALTER TABLE new_project.electrobus_type ADD COLUMN battery_capacity int;

UPDATE new_project.electrobus_type 
SET battery_capacity = 81
WHERE type_id in (1, 3);

UPDATE new_project.electrobus_type 
SET battery_capacity = 70
WHERE type_id in (2, 4);