import sqlite3

def connect_to_database():
    connect = sqlite3.connect(r'lab_4\tasks.db')
    connect.row_factory = sqlite3.Row
    return connect