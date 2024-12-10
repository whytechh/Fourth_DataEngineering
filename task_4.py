import csv
import msgpack
import json
from db_connect import connect_to_database


def create_table(db):
    cursor = db.cursor()
    cursor.execute("""drop table if exists products""")
    cursor.execute("""
    create table if not exists products (
    id INTEGER PRIMARY KEY,
    name STRING NOT NULL,
    price REAL NOT NULL CHECK (price >= 0),
    quantity INTEGER NOT NULL CHECK (quantity >= 0),
    fromCity TEXT,
    isAvailable BOOLEAN NOT NULL,
    views INTEGER NOT NULL,
    category TEXT NULL,
    update_count INTEGER DEFAULT 0
    )
    """)
    db.commit()


def read_items():
    with open(r'lab_4\task_4_solve_result\_product_data.csv', 'r+', encoding='utf-8') as file:
        reader = csv.reader(file, delimiter=';')
        reader.__next__()
        items = []
        for row in reader:
            if len(row) == 0:
                continue
            if len(row) == 7:
                item = {
                    'name': row[0],
                    'price': float(row[1]),
                    'quantity': int(row[2]),
                    'fromCity': row[4],
                    'category': row[3],
                    'isAvailable': bool(row[5]),
                    'views': int(row[6])
                }
            else:
                item = {
                    'name': row[0],
                    'price': float(row[1]),
                    'quantity': int(row[2]),
                    'fromCity': row[3],
                    'isAvailable': bool(row[4]),
                    'views': int(row[5]),
                    'category': None
                }
            items.append(item)

        return items
    

def insert_data(db, data):
    cursor = db.cursor()
    cursor.executemany("""
    insert into products (name, price, quantity, fromCity, isAvailable, views, category)
    values (:name, :price, :quantity, :fromCity, :isAvailable, :views, :category)
    """, data)
    db.commit()


def read_updates():
    with open(r'lab_4\task_4_solve_result\_update_data.msgpack', 'rb') as file:
        updates = msgpack.unpack(file)
    return updates


def apply_update(db, update):
    cursor = db.cursor()
    method = update.get('method')
    param = update.get('param')
    name = update.get('name')

    if method == 'available':
        cursor.execute("""
            UPDATE products
            SET isAvailable = ?, update_count = update_count + 1
            WHERE name = ?
        """, (param, name))

    elif method == 'remove':
        cursor.execute("""
            DELETE FROM products WHERE name = ?
        """, (name,))

    elif method == 'price_percent':
        cursor.execute("SELECT price FROM products WHERE name = ?", (name,))
        current_price = cursor.fetchone()
        if current_price:
            new_price = current_price[0] * (1 + param / 100)
            cursor.execute("""
                UPDATE products
                SET price = ?, update_count = update_count + 1
                WHERE name = ?
            """, (new_price, name))

    elif method == 'quantity_add':
        cursor.execute("SELECT quantity FROM products WHERE name = ?", (name,))
        current_quantity = cursor.fetchone()
        if current_quantity:
            new_quantity = current_quantity[0] + param
            cursor.execute("""
                UPDATE products
                SET quantity = ?, update_count = update_count + 1
                WHERE name = ?
            """, (new_quantity, name))

    elif method == 'quantity_sub':
        cursor.execute("SELECT quantity FROM products WHERE name = ?", (name,))
        current_quantity = cursor.fetchone()
        if current_quantity:
            new_quantity = current_quantity[0] - param
            if new_quantity >= 0:
                cursor.execute("""
                    UPDATE products
                    SET quantity = ?, update_count = update_count + 1
                    WHERE name = ?
                """, (new_quantity, name))

    elif method == 'price_abs':
        if param >= 0:
            cursor.execute("""
                UPDATE products
                SET price = ?, update_count = update_count + 1
                WHERE name = ?
            """, (param, name))

    db.commit()


def process_updates(db, updates):
    for update in updates:
            apply_update(db, update)


def first_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
        SELECT *
        FROM products
        ORDER BY update_count DESC
        LIMIT 10
    ''')
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    return items


def second_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
        SELECT quantity,
        SUM(quantity) as sum_quantity,
        MIN(quantity) as min_quantity,
        MAX(quantity) as max_quantity,
        AVG(quantity) as avg_quantity
        FROM products
        ''')
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    return items 


def third_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
        SELECT category,
        COUNT(*) as count,
        sum(views) as sum_views,
        min(views) as min_views,
        max(views) as max_views,
        avg(views) as avg_views
        FROM products
        GROUP BY category
        ''')
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    return items 


def fourth_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
        SELECT fromCity,
        COUNT(*) as count
        FROM products
        GROUP BY fromCity
    ''')
    items = []
    for row in result.fetchall():
        items.append(dict(row))

    return items 


def save_items(filename, items):
    with open(filename, 'w', encoding='utf-8') as file:
        json.dump(items, file, ensure_ascii=False, indent=1)


db = connect_to_database()

create_table(db)
data = read_items()
insert_data(db, data)
updates = read_updates()

process_updates(db, updates)

save_items(r'lab_4\task_4_solve_result\task_4_most_updatable.json', first_query(db))
save_items(r'lab_4\task_4_solve_result\task_4_quantity_stats.json', second_query(db))
save_items(r'lab_4\task_4_solve_result\task_4_views_stats.json', third_query(db))
save_items(r'lab_4\task_4_solve_result\task_4_city_freq.json', fourth_query(db))

db.close()