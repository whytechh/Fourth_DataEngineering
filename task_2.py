import json
from db_connect import connect_to_database

def read_json():
    with open('task_2_solve_result/subitem.json', 'r', encoding='utf-8') as file:
        return json.load(file)


def create_table(db):
    db.execute("""drop table if exists houses_stats;""")
    db.execute('''
        create table if not exists houses_stats (
        id INTEGER PRIMARY KEY,
        name TEXT references houses(name),
        rating FLOAT,
        convenience INTEGER,
        security INTEGER,
        functionality INTEGER,
        comment TEXT
        )''')


def insert_data(db, data):
    db.executemany('''
    INSERT OR IGNORE INTO houses_stats (name, rating, convenience, security, functionality, comment) 
    VALUES (:name, :rating, :convenience, :security, :functionality, :comment)''', data)
    db.commit()


def first_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
        SELECT *
        FROM houses_stats
        WHERE name = 'Ангар 23'
        ORDER BY security
        ''')
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    return items


def second_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
        SELECT h.name, s.rating, s.convenience, h.prob_price
        FROM houses h
        JOIN houses_stats s ON h.name = s.name
        WHERE s.rating > 2.9
        LIMIT 50
        ''')
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    return items


def third_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
        SELECT h.name, max(h.prob_price) as max_offer, count(s.comment) as reviews
        FROM houses h
        JOIN houses_stats s ON h.name = s.name
        GROUP BY h.name
        ORDER BY s.rating DESC
        LIMIT 50
        ''')
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    return items


def save_items(filename, items):
    with open(filename, 'w', encoding='utf-8') as file:
        json.dump(items, file, ensure_ascii=False, indent=1)


data = read_json()

db = connect_to_database()

create_table(db)

insert_data(db, data)
db.commit()

save_items('task_2_solve_result/task_2_query_1', first_query(db))
save_items('task_2_solve_result/task_2_query_2', second_query(db))
save_items('task_2_solve_result/task_2_query_3', third_query(db))

db.close()
