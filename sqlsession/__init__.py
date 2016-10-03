import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import Table
from sqlalchemy.sql.expression import insert, select, update, delete
from sqlalchemy import and_
from sqlalchemy.exc import IntegrityError
from itertools import imap

def create_engine(params):

    def get_value(data, keys, default=None):
        result = None

        for k in keys:
            result = data.get(k)
            
            if result is not None:
                return result

        if result is None:
            return default

    db_type = get_value(params, ['type', 'db_type'], 'pgsql')

    if db_type == 'mysql':
        default_port = 3306

    elif db_type == 'pgsql':
        default_port = 5432

    ctx = (get_value(params, ['user']),
           get_value(params, ['passwd', 'password', 'pass']),
           get_value(params, ['host', 'server'], 'localhost'),
           get_value(params, ['port'], default_port),
           get_value(params, ['database', 'db_name', 'database_name', 'db']))

    if db_type == 'pgsql':
        url = 'postgresql+psycopg2://%s:%s@%s:%s/%s' % ctx

    elif db_type == 'mysql':
        url = 'mysql+mysqldb://%s:%s@%s:%s/%s' % ctx

    else:
        raise ValueError('db_type must be eighter "mysql" or "pgsql"')

    return sqlalchemy.create_engine(url)
 
   
def preprocess_table_data(table, data):

    if isinstance(data, dict):
        data = [data]

    def convert(item):
        result = {}
        for column in table.columns:
            key = column.name
            value = item.get(unicode(key))

            if value is not None:
                result[key] = value

        return result

    return map(convert, data)


def build_pkey_condition(table, data):
    pkeys = table.primary_key.columns
    condition = []

    for column in pkeys:
        condition.append(column == data[column.name])

    return and_(*condition)


def build_condition_from_dict(table, dict_condition):
    condition = []    

    for key,value in dict_condition.items():
        column = getattr(table.columns, key)
        condition.append(column == value)

    return and_(*condition)


class SqlSessionNotFound(Exception):
    pass


class SqlSessionTooMany(Exception):
    pass


class SqlSession(object):

    def __init__(self, param = None):
        
        self.column_names = None
        
        def init(p):
            self.engine = create_engine(p)
            self.metadata = sqlalchemy.MetaData(self.engine)
            self.session_cls = sessionmaker(bind=self.engine)
      
        if isinstance(param, dict):
            init(param)

        elif param is not None:
            self.session_cls = param

        else:
            raise ValueError("No parameters to initialize Session")

    def __enter__(self):
        self.session = self.session_cls()
        return self

    def __exit__(self, type, value, traceback):
        self.session.expunge_all()
        self.session.close()

    def get_table(self, schema_table_name):
        t = schema_table_name.split('.')
        
        if len(t) == 1:
            table_name = t[0]
            return Table(table_name, self.metadata, autoload=True,
                         autoload_with=self.session.get_bind())

        elif len(t) == 2:
            schema_name, table_name = t
            return Table(table_name, self.metadata, autoload=True,
                         autoload_with=self.session.get_bind(),
                         schema=schema_name)

        else:
            raise ValueError("schema_table_name")

    def update(self, table, data, condition=None):

        if isinstance(table, str):
            table = self.get_table(table)

        if condition is None:
            condition = build_pkey_condition(table, data)

        elif isinstance(condition, dict):
            condition = build_condition_from_dict(table, condition)

        data = preprocess_table_data(table, data)
        stmt = update(table).where(condition).values(data[0])
        return self.session.execute(stmt)

    def insert(self, table, data):

        if isinstance(table, str):
            table = self.get_table(table)

        data = preprocess_table_data(table, data)       
        stmt = insert(table, data)
        return self.session.execute(stmt)

    def delete(self, table, condition=None):
        if isinstance(table, str):
            table = self.get_table(table)

        if condition is not None:
            stmt = delete(table_name).where(condition)

        elif isinstance(condition, dict):
            condition = build_condition_from_dict(table, condition)
            stmt = delete(table_name).where(condition)

        else:
            return 

        return self.session.execute(stmt)

    def execute(self, statement):
        return self.session.execute(statement)

    def commit(self):
        return self.session.commit()

    def get_statement(self, table, condition):
        
        if isinstance(table, str) or isinstance(table, unicode):
            table = self.get_table(table)

        stmt = table.select()

        if isinstance(condition, dict):
            condition = build_condition_from_dict(table, condition)

        if condition is not None:
            stmt = stmt.where(condition)

        return stmt

    def fetch_one(self, table, condition):
        if isinstance(table, str) or isinstance(table, unicode):
            table = self.get_table(table)

        stmt = table.select()

        if isinstance(condition, dict):
            condition = build_condition_from_dict(table, condition)
    
        if condition is not None:
            stmt = stmt.where(condition)

        return self.one(stmt)

    def fetch_all(self, table, condition=None):
        stmt = self.get_statement(table, condition)
        return self.all(stmt)

    def one(self, statement):
        data = self.session.execute(statement)
        self.column_names = data.keys()
        data = map(dict, data)

        if len(data) > 1:
            raise SqlSessionTooMany("Expected exaclty one record, %s found" % len(data))

        elif len(data) == 0:
            raise SqlSessionNotFound("Row not found")

        return data[0]

    def all(self, statement):
        data = self.session.execute(statement)
        self.column_names = data.keys()
        result = imap(dict, data)
        return result

    def drop_table(table):
        if isinstance(table, str):
            table = self.get_table(table)
            
        table.drop()
