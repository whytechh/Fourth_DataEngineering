import pandas as pd
import pickle
import json
from db_connect import connect_to_database


def create_books_table(db):
    with open('./task_5_solve_result/metadata.json', 'r', encoding='utf-8') as file:
        data = json.load(file)
        books = []
        for row in data:
            if len(row) == 0: continue
            books.append({
                'book_id': int(row['item_id']),
                'url': row['url'],
                'title': row['title'],
                'authors': row['authors'],
                'language': row['lang'] if 'lang' in row and not pd.isna(row['lang']) else None,
                'image': row['img'],
                'year': int(row['year']),
                'description': row['description'] if 'description' in row and not pd.isna(row['description']) else None
            })
        cursor = db.cursor()
        cursor.execute('''drop table if exists books''')
        cursor.execute('''
        create table if not exists books (
        id integer primary key,
        book_id integer,
        url text,
        title text,
        authors text,
        language text,
        image TEXT,
        year integer,
        description text)
        ''')
        cursor.executemany('''
        insert into books (book_id, url, title, authors, language, image, year, description)
        values (:book_id, :url, :title, :authors, :language, :image, :year, :description)
        ''', books)
        db.commit()
        return books
    

def create_ratings_table(db):
    with open('./task_5_solve_result/ratings.pkl', 'rb') as file:
        data = pickle.load(file)
        data = data.rename(columns={'item_id': 'book_id'})
        ratings = data[['book_id', 'user_id', 'rating']].to_dict(orient='records')
        cursor = db.cursor()
        cursor.execute('''drop table if exists ratings''')
        cursor.execute('''
        create table if not exists ratings (
        id integer primary key,
        book_id integer references books(book_id),
        user_id integer,
        rating integer)
        ''')
        cursor.executemany('''
        insert into ratings (book_id, user_id, rating)
        values (:book_id, :user_id, :rating)
        ''', ratings)
        db.commit()
        return ratings
    

def create_reviews_table(db):
        data = pd.read_csv('./task_5_solve_result/reviews.csv', encoding='utf-8')
        data = data.rename(columns={'item_id': 'book_id', 'txt': 'review'})
        reviews = data[['book_id', 'review']].to_dict(orient='records')
        cursor = db.cursor()
        cursor.execute('''drop table if exists reviews''')
        cursor.execute('''
        create table if not exists reviews (
        id integer primary key,
        book_id integer references ratings(book_id),
        review text)
        ''')
        cursor.executemany('''
        insert into reviews (book_id, review)
        values (:book_id, :review)
        ''', reviews)
        db.commit()
        return reviews


def first_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
    select books.book_id, books.title, avg(ratings.rating) as avg_rating
    from books
    join ratings on books.book_id = ratings.book_id
    where year = 2003
    group by books.book_id
    order by avg_rating desc
    limit 30
    ''')
    query = []
    for row in result.fetchall():
        query.append(dict(row))
    return query


def second_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
    select books.book_id, books.title, books.authors, COUNT(reviews.review) as review_count
    from books
    join reviews on books.book_id = reviews.book_id
    group by books.title
    order by review_count desc
    limit 10
    ''')
    query = []
    for row in result.fetchall():
        query.append(dict(row))
    return query


def third_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
    select
    ratings.book_id,
    min(ratings.rating) as min_rating,
    max(ratings.rating) as max_rating,
    avg(ratings.rating) as avg_rating,
    count(distinct ratings.user_id) as reviewers
    from ratings
    join reviews on ratings.book_id = reviews.book_id
    group by reviews.book_id
    order by reviewers desc
    limit 10
    ''')
    query = []
    for row in result.fetchall():
        query.append(dict(row))
    return query


def fourth_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
    select
    books.authors,
    avg(ratings.rating) as avg_rating,
    count(distinct ratings.user_id) as reviewers
    from books
    join ratings on books.book_id = ratings.book_id 
    group by books.authors                       
    order by reviewers desc
    limit 100
    ''')
    query = []
    for row in result.fetchall():
        query.append(dict(row))
    return query


def fifth_query(db):
    cursor = db.cursor()
    cursor.execute('''
    delete from books
    where title like "%dead%"
    or title like "%suicide%"
    or title like "%drug%"
    or title like "%sex%"
    ''')
    db.commit()

    rows_deleted = cursor.rowcount
    print(f"Удалено {rows_deleted} книг, содержащих контент для взрослых.")


def sixth_query(db):
    cursor = db.cursor()
    cursor.execute('''
    update ratings
    set rating = case
        when rating = 1 then rating + 1
        when rating < 5 then rating + 0.5
        else rating
    end
    where rating < 5
    ''')
    db.commit()

    updated_rows = cursor.rowcount
    print(f"Количество обновленных строк рейтинга: {updated_rows}")
    return 


def save_queries(filename, items):
    with open(filename, 'w', encoding='utf-8') as file:
        json.dump(items, file, ensure_ascii=False, indent=1)


db = connect_to_database()

books = create_books_table(db)
ratings = create_ratings_table(db)
reviews = create_reviews_table(db)

# save_queries('./task_5_solve_result/task_5_query_1.json', first_query(db))
# save_queries('./task_5_solve_result/task_5_query_2.json', second_query(db))
# save_queries('./task_5_solve_result/task_5_query_3.json', third_query(db))
# save_queries('./task_5_solve_result/task_5_query_4.json', fourth_query(db))
fifth_query(db)
sixth_query(db)
