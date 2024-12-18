# Лабораторная работа №4.  
Вариант 75.  

## Fifth task  

Предметная область выбранного датасета - книги.

Файл metadata.json содержит информацию о книгах.  
Поля:  
item_id - идентификатор книги;  
url - ссылка на книгу;  
authors - автор(ы) книги;  
lang - язык, на котором написана книга;  
img - ссылка на картинку обложки книги;  
year - год выпуска книги;  
description - аннотация.

Файл ratings.pkl содержит информацию о рейтингах.  
Поля:  
item_id - идентификатор книги;  
user_id - идентификатор пользователя;  
rating - оценка книги.

Файл reviews.csv содержит информацию о отзывах пользователей.  
Поля:  
item_id - идентификатор книги;  
txt - отзыв пользователя.

### Результаты

Скрипт task_5.py пересоздает таблицы и сохраняет их в БД tasks.db (как и все остальные таблицы).

Результаты запросов:  
1. task_5_query_1.json - выборка топ-30 книг за 2003 год с их средним рейтингом;
2. task_5_query_2.json - выборка топ-10 самых обсуждаемых книг (по отзывам, т. к. они не уникальны, поскольку один пользователь оставлял несколько отзывов);
3. task_5_query_3.json - выборка топ-10 самых обсуждаемых книг [по количеству комментаторов] (макс.\мин.\сред. рейтинг, количество пользователей, написавших отзыв);
4. task_5_query_4.json - выборка топ-100 авторов по популярности (средний рейтинг и количество пользователей, оставивших отзывы под их произведениями);
5. fifth_query (без файла) - удаление книг из БД, содержащих контент для взрослых в названии (например, слово suicide);
6. sixth_query (без файла) - обновление рейтинга всех книг, кроме тех, которые имеют самый высокий.
