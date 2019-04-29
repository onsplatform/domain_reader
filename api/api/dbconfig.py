from pony import orm

db = orm.Database()

sqlite_path = '..\\platform_sdk\\db.sqlite3'
db.bind(provider='sqlite', filename=sqlite_path)