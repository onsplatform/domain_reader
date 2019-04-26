from pony import orm

db = orm.Database()

sqlite_path = 'D:\\projetos\\domain_reader\\sdk\\db.sqlite3'
db.bind(provider='sqlite', filename=sqlite_path)