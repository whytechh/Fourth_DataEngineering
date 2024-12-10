import csv
import json
from db_connect import connect_to_database


def read_csv():
    items = []
    with open(r'lab_4\task_3_solve_result\_part_1.csv', 'r', encoding='utf-8') as file:
        data = csv.reader(file, delimiter=';')
        data.__next__()
        for row in data:
            if len(row) == 0:
                continue
            items.append({
                'artist': row[0],
                'song': row[1],
                'duration_ms': int(row[2]),
                'year': int(row[3]),
                'tempo': float(row[4]),
                'genre': row[5],
                'energy': float(row[6]),
                'key': int(row[7]),
                'loudness': float(row[8]),
            })
    return items


def read_json():
    items = []
    with open(r'lab_4\task_3_solve_result\_part_2.json', 'r', encoding='utf-8') as file:
        data = json.load(file)
        for row in data:
            if len(row) == 0:
                continue
            items.append({
                'artist': row['artist'],
                'song': row['song'],
                'duration_ms': int(row['duration_ms']),
                'year': int(row['year']),
                'tempo': float(row['tempo']),
                'genre': row['genre'],
                'explicit': bool(row['explicit']),
                'popularity': int(row['popularity']),
                'danceability': float(row['danceability']),
            })
    return items


def create_table(db):
    db.execute("""drop table if exists songs;""")
    db.execute("""
    CREATE TABLE IF NOT EXISTS songs 
    (id INTEGER PRIMARY KEY,
    artist text,
    song text,
    duration_ms INTEGER,
    year INTEGER,
    tempo real,
    genre text
    )""")


def insert_data(db, data):
    db.executemany('''
    INSERT OR IGNORE INTO songs (artist, song, duration_ms, year, tempo, genre) 
    VALUES (:artist, :song, :duration_ms, :year, :tempo, :genre)''', data)
    db.commit()


def first_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
        SELECT *
        FROM songs
        WHERE tempo > 180
        LIMIT 85
        ''')
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    return items


def second_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
        SELECT sum(duration_ms) as sum_duration_ms,
               min(duration_ms) as min_duration_ms,
               max(duration_ms) as max_duration_ms,
               avg(duration_ms) as avg_duration_ms
        FROM songs
        WHERE tempo > 180
        ''')
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    return items


def third_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
        SELECT
            COUNT(*) as count,
            genre
        FROM songs
        GROUP BY genre
        ''')
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    return items


def fourth_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
        SELECT * 
        FROM songs
        WHERE year > 2007
        ORDER BY artist
        LIMIT 90
        ''')
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    return items


def save_items(filename, items):
    with open(filename, 'w', encoding='utf-8') as file:
        json.dump(items, file, ensure_ascii=False, indent=1)


data_csv = read_csv()
data_json = read_json()

db = connect_to_database()

create_table(db)
insert_data(db, data_csv)
insert_data(db, data_json)

save_items(r'lab_4\task_3_solve_result\task_3_filtered.json', first_query(db))
save_items(r'lab_4\task_3_solve_result\task_3_stats.json', second_query(db))
save_items(r'lab_4\task_3_solve_result\task_3_freq.json', third_query(db))
save_items(r'lab_4\task_3_solve_result\task_3_ordered_and_filtered.json', fourth_query(db))
