from pony import orm

db = orm.Database()

sqlite_path = '..\\db.sqlite3'
db.bind(provider='sqlite', filename=sqlite_path)
#db.generate_mapping(create_tables=True)