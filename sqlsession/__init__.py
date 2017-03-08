import re
import sqlalchemy
import sqlalchemy.engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import Table
from sqlalchemy.sql.expression import insert, select, update, delete
from sqlalchemy import and_
from sqlalchemy.exc import IntegrityError
from itertools import imap

#TODO: LazySession !!!

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

    engine = sqlalchemy.create_engine(url)
    engine.update_execution_options(execution_options={'stream_results': True})
    return engine
 
   
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


def build_order_from_list(table, order_list):
    
    def get_column(key, direction):

        if direction is not None and direction not in ('desc', 'asc'):
            raise ValueError("Order direction must be 'desc' or 'asc'")

        if direction == 'desc':
            return getattr(table.columns, key).desc()

        else:
            return getattr(table.columns, key)

    def interpret_column(column):

        if isinstance(order_list, tuple):
            return get_column(order_list[1], order_list[0])

        if isinstance(order_list, str) or isinstance(order_list, unicode):
            return get_column(order_list, 'asc')

        else:
            raise ValueError('Can not interpret order statement. Use list of strings or tuples.')

    if isinstance(order_list, list):
        return list(map(interpret_column, order_list))

    else:
        return [interpret_column(order_list)]



class SqlSessionNotFound(Exception):
    pass


class SqlSessionTooMany(Exception):
    pass


class SqlSession(object):
    
    def __init__(self, param = None, as_role=None):
        
        self.column_names = None
        self.transaction = None
        self.as_role = as_role
        
        if isinstance(param, dict):
            self.engine = create_engine(param)
            self.metadata = sqlalchemy.MetaData(self.engine)

        elif isinstance(param, sqlalchemy.engine.Engine):
            self.engine = param
            self.metadata = sqlalchemy.MetaData(self.engine)

        else:
            raise ValueError("No parameters to initialize Session")

    def __enter__(self):
        self.connection = self.engine.connect()

        if self.as_role is not None:
            self.set_role(self.as_role)

        return self

    def __exit__(self, type, value, traceback):
        if self.transaction is not None:
            self.transaction.commit()
            self.transaction = None

        self.connection.close()

    def begin(self):
        self.transaction = self.connection.begin()

    def end(self):
        if self.transaction is not None:
            self.transaction.commit()
            self.transaction.close()
            self.transaction = None

    def rollback(self):
        if self.transaction is not None:
            self.transaction.rollback()
            self.transaction.close()
            self.transaction = None

    def get_unbound_connection(self):
        return self.engine.contextual_connect(close_with_result=True).execution_options(stream_results=True)

    def get_table(self, schema_table_name):
        t = schema_table_name.split('.')
        
        if len(t) == 1:
            table_name = t[0]
            return Table(table_name, self.metadata, autoload=True,
                         autoload_with=self.engine)

        elif len(t) == 2:
            schema_name, table_name = t
            return Table(table_name, self.metadata, autoload=True,
                         autoload_with=self.engine,
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
        return self.connection.execute(stmt)

    def insert(self, table, data):

        if isinstance(table, str):
            table = self.get_table(table)

        data = preprocess_table_data(table, data)       
        stmt = insert(table, data)
        return self.connection.execute(stmt)

    def delete(self, table, condition=None):

        if isinstance(table, str):
            table = self.get_table(table)

        if isinstance(condition, dict):
            condition = build_condition_from_dict(table, condition)
            stmt = delete(table).where(condition)
            self.connection.execute(stmt)

    def truncate(self, table):
        raise RuntimeError('Not yet inmplement')

    def execute(self, statement):
        return self.connection.execute(statement)

    def commit(self):
        return self.connection.commit()

    def get_statement(self, table, condition, order):
        
        if isinstance(table, str) or isinstance(table, unicode):
            table = self.get_table(table)

        stmt = table.select()

        if isinstance(condition, dict):
            condition = build_condition_from_dict(table, condition)

        if condition is not None:
            stmt = stmt.where(condition)

        if order is not None:
            stmt = stmt.order_by(*build_order_from_list(table, order))

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

    def fetch_all(self, table, condition=None, order=None):
        stmt = self.get_statement(table, condition, order)
        return self.all(stmt)

    def one(self, statement):
        data = self.connection.execute(statement)
        self.column_names = data.keys()
        data = map(dict, data)

        if len(data) > 1:
            raise SqlSessionTooMany("Expected exaclty one record, %s found" % len(data))

        elif len(data) == 0:
            raise SqlSessionNotFound("Row not found")

        return data[0]

    def all(self, statement):
        data = self.get_unbound_connection().execute(statement)
        self.column_names = data.keys()
        result = imap(dict, data)
        return result

    def drop_table(table):
        if isinstance(table, str):
            table = self.get_table(table)
            
        table.drop()

    def add_user(self, user_name):
        if not re.match('[a-zA-Z0-9]*', user_name):
            raise ValueError('User name can contain only letters and numbers')

        self.connection.execute('CREATE USER %s' % user_name)

    def add_group(self, group_name):
        if not re.match('[a-zA-Z0-9]*', group_name):
            raise ValueError('Group name can contain only letters and numbers')

        self.connection.execute('CREATE GROUP %s' % group_name)

    def add_user_to_group(user_name, group_name):
        if not re.match('[a-zA-Z0-9]*', user_name):
            raise ValueError('User name can contain only letters and numbers')

        if not re.match('[a-zA-Z0-9]*', group_name):
            raise ValueError('Group name can contain only letters and numbers')

        self.connection.execute('ALTER GROUP %s ADD USER %s' % group_name)

    def drop_user(self, user_name):
        if not re.match('[a-zA-Z0-9]*', user_name):
            raise ValueError('User name can contain only letters and numbers')

        self.connection.execute('DROP USER %s' % user_name)

    def set_role(self, user_name):
        if not re.match('[a-zA-Z0-9]*', user_name):
            raise ValueError('User name can contain only letters and numbers')

        self.connection.execute('SET role=%s' % user_name)

