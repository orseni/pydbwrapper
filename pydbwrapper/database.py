"""Database access module"""
import os

import psycopg2
import psycopg2.extras

from pydbwrapper.config import Config

QUERIES_DIR = os.path.realpath(os.path.curdir) + '/sql/'

class DictWrapper(dict):
    """Dict wrapper to access dict attributes with . operator"""

    def __init__(self, data):
        self.update(data)

    def __getattr__(self, name):
        if name in self:
            if isinstance(self[name], dict) and not isinstance(self[name], DictWrapper):
                self[name] = DictWrapper(self[name])
            return self[name]
        raise AttributeError('{} is not a valid attribute'.format(name))

    def __setattr__(self, name, value):
        self[name] = value

    def _asdict(self):
        return self

class CursorWrapper(object):

    def __init__(self, cursor):
        self.cursor = cursor

    def fetchone(self):
        row = self.cursor.fetchone()
        if row is not None:
            return DictWrapper(row)
        return row

    def fetchmany(self, size):
        return [DictWrapper(r) for r in self.cursor.fetchmany(size)]

    def fetchall(self):
        return [DictWrapper(r) for r in self.cursor.fetchall()]

    def close(self):
        self.cursor.close()

    def __iter__(self):
        return self

    def __next__(self):
        return DictWrapper(self.cursor.__next__())


class SQLBuilder(object):

    def __init__(self, database, table):
        self.database = database
        self.table = table
        self.set_values = {}
        self.where_values = {}
        self.set_operators = {}
        self.where_operators = {}

    def setall(self, data):
        for value in data.keys():
            self.set(value, data[value])
        return self

    def set(self, field, value, operator='=', constant=False):
        self.set_values[field] = value
        self.set_operators[field] = operator
        return self

    def where(self, field, value, operator='=', constant=False):
        self.where_values[field] = value
        self.where_operators[field] = operator
        return self

    def whereall(self, data):
        for value in data.keys():
            self.where(value, data[value])
        return self

    def sql(self):
        pass

    def parameters(self):
        parameters = self.set_values.copy()
        parameters.update(self.where_values)
        return parameters

    def execute(self):
        print("SQL: {} - PARAMS: {}".format(self.sql(), self.parameters()))
        return self.database.execute(self.sql(), self.parameters())


class SelectBuilder(SQLBuilder):

    def __init__(self, database, table):
        super(SelectBuilder, self).__init__(database, table)
        self.fields = {}
        self.page_str = ""

    def sql(self):
        formato = '{0} {1} %({0})s'
        where_str = ' AND '.join([formato.format(field, self.where_operators[field]) for field in self.where_values])
        if where_str != '':
            where_str = "WHERE {}".format(where_str)

        return 'SELECT * FROM {} {} {}'.format(self.table, where_str, self.page_str)

    def paging(self, pagenumber, pagesize):
        if pagesize is not None and pagenumber is not None:
            self.page_str = "LIMIT {} OFFSET {}".format(pagesize, pagenumber)
        data = self.execute().fetchall()
        return Page(pagenumber, pagesize, data)


class UpdateBuilder(SQLBuilder):

    def sql(self):
        formato = '{0} {1} %({0})s'
        set_str = ', '.join([formato.format(field, self.set_operators[field]) for field in self.set_values])
        where_str = ' AND '.join([formato.format(field, self.where_operators[field]) for field in self.where_values])
        return 'UPDATE {} SET {} WHERE {}'.format(self.table, set_str, where_str)


class DeleteBuilder(SQLBuilder):

    def sql(self):
        formato = '{0} {1} %({0})s'
        where_str = ' AND '.join([formato.format(field, self.where_operators[field]) for field in self.where_values])
        return 'DELETE FROM {} WHERE {}'.format(self.table, where_str)


class InsertBuilder(SQLBuilder):

    def sql(self):
        cols_str = ', '.join(self.set_values.keys())
        value_str = ', '.join(["%({})s".format(field, self.set_operators[field]) for field in self.set_values])
        return 'INSERT INTO {}({}) VALUES ({})'.format(self.table, cols_str, value_str)


class Page(dict):

    def __init__(self, number, size, data):
        self["number"] = self.number = number
        self["size"] = self.size = size
        self["data"] = self.data = data


class Database(object):
    """Facade to access database using psycopg2"""

    def __init__(self, config=None):
        self.config = Config.instance() if config is None else config
        self.connection = self.config.pool.connection()

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self.connection.commit()
        self.connection.close()

    def __load_query(self, name):
        """Load a query located in ./sql/<name>.sql"""
        with open(QUERIES_DIR + name + '.sql') as query:
            query_string = query.read()
        return query_string

    def execute(self, name_or_sql, parameters=None):
        """Execute query by name returning cursor"""
        cursor = self.connection.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            sql = self.__load_query(name_or_sql)
        except FileNotFoundError:
            sql = name_or_sql
        cursor.execute(sql, parameters)
        return CursorWrapper(cursor)

    def select(self, table):
        return SelectBuilder(self, table)

    def update(self, table):
        return UpdateBuilder(self, table)

    def insert(self, table):
        return InsertBuilder(self, table)

    def delete(self, table):
        return DeleteBuilder(self, table)

    def disconnect(self):
        """Disconnect from database"""
        self.connection.close()
