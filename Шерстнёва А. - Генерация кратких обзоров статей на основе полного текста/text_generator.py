# Импортируем необходимые библиотеки
import requests
from bs4 import BeautifulSoup
import sqlite3
import re
import time
from openai import OpenAI

url = 'https://techcrunch.com/'

# Отправляем GET-запрос к странице
response = requests.get(url)

# Проверяем успешность запроса и парсим HTML-код страницы
if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')
    text_generation = soup.find_all('a', class_='post-block__title__link')

# Создаем подключение к БД
conn = sqlite3.connect('text_generation.db')
cursor = conn.cursor()

# Создаем таблицу для хранения статей
cursor.execute('''CREATE TABLE IF NOT EXISTS text_generation
                (id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT,
                complete_text TEXT,
                url TEXT)''')

# Создаем таблицу для хранения кратких пересказов статей
cursor.execute('''CREATE TABLE IF NOT EXISTS short_table
                (id INTEGER PRIMARY KEY AUTOINCREMENT,
                short_text TEXT,
                article_id INTEGER,
                FOREIGN KEY(article_id) REFERENCES text_generation(id))''')

# Установка API ключа OpenAI
api_key = 'OPENAI_API_KEY'
client = OpenAI(api_key=api_key)

# Ограничение на количество обрабатываемых статей
max_articles = 20
articles_processed = 0

# Переменная для накопления времени выполнения запросов
total_chat_gpt_execution_time = 0

# Цикл по всем статьям
for index, article in enumerate(text_generation):
    if articles_processed >= max_articles:
        break  # Выход из цикла после обработки максимального количества статей
    
    article_url = article['href']
    article_response = requests.get(article_url)

    if article_response.status_code == 200:
        article_soup = BeautifulSoup(article_response.content, 'html.parser')
        article_title_element = article_soup.find('h1', class_='article__title')
        article_text_element = article_soup.find('div', class_='article-content')

        if article_title_element and article_text_element:
            article_title = article_title_element.text
            article_text = article_text_element.text

            # Вставка данных статьи в таблицу
            cursor.execute("INSERT INTO text_generation (title, complete_text, url) VALUES (?, ?, ?)", 
                           (article_title, article_text, article_url))
            conn.commit()
            print('Title:', article_title)
            print('Text:', article_text)
            print('URL:', article_url)

            # Время начала выполнения запроса к API chat GPT
            start_chat_gpt_time = time.time()

            # Вызываем API для создания краткого пересказа
            completion = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "Ты переводчик и пересказчик."},
                    {"role": "user", "content": f"Сделай краткий пересказ текста и переведи краткий пересказ на русский язык:\n\n{article_text}"}
                ]
            )
            short_text = completion.choices[0].message
            short_text = str(short_text)

            # Время окончания выполнения запроса к API chat GPT
            end_chat_gpt_time = time.time()

            # Подсчет времени выполнения этапа обработки запроса к API chat GPT
            chat_gpt_execution_time = end_chat_gpt_time - start_chat_gpt_time
            # Добавляем время выполнения текущего запроса к общему времени
            total_chat_gpt_execution_time += chat_gpt_execution_time

            # Обработка данных, удаление лишних символов
            short_text = short_text.replace('ChatCompletionMessage(content=', '')
            short_text = re.sub(r',? role=\'assistant\', function_call=None, tool_calls=None', '', short_text)
            short_text = short_text.replace('Краткий пересказ:', '')
            short_text = short_text.replace('Пересказ:', '')
            short_text = short_text.replace('\\n', '\n')
            short_text = short_text.strip('\'')
            short_text = short_text.strip('\')')

            # Получение идентификатора последней добавленной статьи
            cursor.execute("SELECT last_insert_rowid()")
            article_id = cursor.fetchone()[0]

            # Вставка данных краткого пересказа в таблицу
            cursor.execute("INSERT INTO short_table (short_text, article_id) VALUES (?, ?)", (short_text, article_id))
            conn.commit()
            print('Short Text:', short_text)
            print('---')

            # Увеличение счетчика обработанных статей
            articles_processed += 1

# Закрываем соединение с базой данных
conn.close()

 # Вычисляем среднее значение времени выполнения
average_chat_gpt_execution_time = total_chat_gpt_execution_time / max_articles

# Выводим среднее значение
print(f"Среднее время выполнения обработки запроса к API chat GPT: {average_chat_gpt_execution_time} секунд")