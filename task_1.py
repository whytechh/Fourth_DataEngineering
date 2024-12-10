import json
from db_connect import connect_to_database

def load_text():
    with open('task_1_solve_result/item.text', 'r', encoding='utf-8') as file:
        return file.read().strip()


def parse_block(block):
    data = {}
    lines = block.strip().split('\n')
    for line in lines:   
        if '::' in line:
            key, value = line.split('::', 1)
            data[key.strip()] = value.strip()
    data['id'] = int(data.get('id', 0))
    data['zipcode'] = int(data.get('zipcode', 0))
    data['floors'] = int(data.get('floors', 0))
    data['year'] = int(data.get('year', 0))
    data['prob_price'] = int(data.get('prob_price', 0))
    data['views'] = int(data.get('views', 0))
    data['parking'] = data.get('parking', 'False') == 'True'
    return data


def process_data(content):
    blocks = content.split('=====')
    parsed_data = []
    for block in blocks:
        block = block.strip()
        if block:
            parsed_data.append(parse_block(block))
    return parsed_data


def create_table(db):
    db.execute('''
        CREATE TABLE IF NOT EXISTS houses (
            id INTEGER PRIMARY KEY,
            name TEXT,
            street TEXT,
            city TEXT,
            zipcode INTEGER,
            floors INTEGER,
            year INTEGER,
            parking INTEGER,
            prob_price INTEGER,
            views INTEGER
        )''')


def insert_data(db, data):
    db.executemany('''
    INSERT OR REPLACE INTO houses (id, name, street, city, zipcode, floors, year, parking, prob_price, views) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', data)
    db.commit()


def first_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
        SELECT *
        FROM houses
        ORDER BY prob_price
        LIMIT 85
    ''')
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    return items


def second_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
        SELECT
            COUNT(*) as count,
            MIN(views) as min_views,
            MAX(views) as max_views,
            ROUND(AVG(views), 2) as avg_views
        FROM houses''')
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    return items    


def third_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
        SELECT
            COUNT(*) as count,
            city
        FROM houses
        GROUP BY city
    ''')
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    return items 


def fourth_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
        SELECT *
        FROM houses
        WHERE year > 1955
        ORDER BY year DESC
        LIMIT 85
    ''')

    items = []

    for row in result.fetchall():
        items.append(dict(row))

    return items 


def save_items(filename, items):
    with open(filename, 'w', encoding='utf-8') as file:
        json.dump(items, file, ensure_ascii=False, indent=1)


data = load_text()
parsed_data = process_data(data)

data_to_insert = [(item['id'], item['name'], item['street'], item['city'], item['zipcode'], 
                  item['floors'], item['year'], int(item['parking']), item['prob_price'], item['views']) 
                  for item in parsed_data]

db = connect_to_database()
create_table(db)
insert_data(db, data_to_insert)
db.commit()

save_items('task_1_solve_result/task_1_sorted_prob_price.json', first_query(db))
save_items('task_1_solve_result/task_1_stats_views.json', second_query(db))
save_items('task_1_solve_result/task_1_freq_city.json', third_query(db))
save_items('task_1_solve_result/task_1_year_filtered.json', fourth_query(db))

db.close()
