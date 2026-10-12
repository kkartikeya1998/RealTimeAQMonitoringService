import sqlite3

conn = sqlite3.connect("local_data/app.db")
cursor = conn.cursor()

cursor.execute("SELECT * FROM locations")
print(cursor.fetchall())

cursor.execute("SELECT * FROM measurements")
print(cursor.fetchall())

conn.close()