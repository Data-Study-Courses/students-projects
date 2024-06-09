-- переименование столбца text
ALTER TABLE text_generation 
RENAME COLUMN text TO complete_text;

-- удаляем дубликаты
DELETE FROM text_generation 
WHERE id NOT IN (
SELECT MIN(id)
FROM text_generation 
GROUP BY title, complete_text 
);

/* создание итоговой таблицы, которая содержит 
полный текст, краткий пересказ и url*/
CREATE TABLE IF NOT EXISTS final_table(
id INTEGER PRIMARY KEY AUTOINCREMENT,
full_text TEXT,
short_text TEXT,
url TEXT,
FOREIGN KEY (id) REFERENCES text_generation(id)
);

-- вставка данных в таблицу
INSERT INTO final_table(full_text, short_text, url) 
SELECT text_generation.complete_text, short_table.short_text , text_generation.url  
FROM text_generation 
JOIN short_table on text_generation.id  = short_table.article_id ;

-- удаление дубликатов  по url 
DELETE FROM final_table  
WHERE id NOT IN (
SELECT MIN(id)
FROM final_table  
GROUP BY url
);

-- удаление пустых значений
DELETE FROM final_table  WHERE full_text IS NULL OR full_text = '';


-- количество символов в полном и кратком текстах
SELECT id, 
LENGTH(full_text) AS full_text_symbols,
LENGTH(short_text) AS short_text_symbols
FROM final_table;

-- средняя длина текстов статей
SELECT ROUND(AVG(LENGTH(full_text)), 2) 
AS avg_full,
ROUND(AVG (LENGTH(short_text)), 2) 
AS avg_short
FROM final_table ft;  

-- процент общей длины коротких пересказов к общей длине полных текстов
SELECT SUM(LENGTH(short_text)) 
AS total_short_text_length,
SUM(LENGTH(full_text)) 
AS total_full_text_length,
ROUND((SUM(LENGTH(short_text)) * 100.0 / SUM(LENGTH(full_text))),1) 
AS percent
FROM final_table;




