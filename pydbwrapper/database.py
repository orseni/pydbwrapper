"""Database access module"""
import os

import psycopg2
import psycopg2.extras
import errno

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
        else:
            self.close()
        return row

    def fetchmany(self, size):
        return [DictWrapper(r) for r in self.cursor.fetchmany(size)]

    def fetchall(self):
        return [DictWrapper(r) for r in self.cursor.fetchall()]

    def close(self):
        self.cursor.close()

    def rowcount(self):
        return self.cursor.rowcount

    def next(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration()
        return row

    def __next__(self):
        return self.next()

    def __iter__(self):
        return self


class SQLBuilder(object):

    def __init__(self, database, table):
        self.database = database
        self.table = table
        self.where_conditions = []
        self.parameters = {}

    def where(self, field, value, operator='=', constant=False):
        if constant:
            self.where_conditions.append(
                '{} {} {}'.format(field, operator, value))
        else:
            self.where_conditions.append(
                '{0} {1} %({0})s'.format(field, operator))
            self.parameters[field] = value
        return self

    def whereall(self, data):
        for value in data.keys():
            self.where(value, data[value])
        return self

    def sql(self):
        pass

    def build_where(self):
        if len(self.where_conditions) > 0:
            conditions = ' AND '.join(self.where_conditions)
            return 'WHERE {}'.format(conditions)
        else:
            return ''

    def execute(self):
        return self.database.execute(self.sql(), self.parameters, True)


class SelectBuilder(SQLBuilder):

    def __init__(self, database, table):
        super(SelectBuilder, self).__init__(database, table)
        self.fields_to_select = ['*']
        self.page_str = ''
        self.order_by_fields = []
        self.group_by_fields = []

    def fields(self, *fields):
        self.fields_to_select = fields
        return self

    def order_by(self, *fields):
        self.order_by_fields = fields
        return self

    def group_by(self, *fields):
        self.group_by_fields = fields
        return self

    def sql(self):
        order_by_str = ', '.join(self.order_by_fields)
        if order_by_str != '':
            order_by_str = 'ORDER BY {}'.format(order_by_str)

        group_by_str = ', '.join(self.group_by_fields)
        if group_by_str != '':
            group_by_str = 'GROUP BY {}'.format(group_by_str)

        return 'SELECT {} FROM {} {} {} {} {}'.format(
            ', '.join(self.fields_to_select),
            self.table,
            self.build_where(),
            group_by_str,
            order_by_str,
            self.page_str)

    def paging(self, page=0, page_size=10):
        self.page_str = "LIMIT {} OFFSET {}".format(
            page_size + 1, page * page_size)
        data = self.execute().fetchall()
        last_page = len(data) <= page_size
        return Page(page, page_size, data[:-1] if not last_page else data, last_page)


class UpdateBuilder(SQLBuilder):

    def __init__(self, database, table):
        super(UpdateBuilder, self).__init__(database, table)
        self.set_statements = []

    def sql(self):
        return 'UPDATE {} {} {}'.format(self.table, self.build_set(), self.build_where())

    def build_set(self):
        if len(self.set_statements) > 0:
            statements = ', '.join(self.set_statements)
            return 'SET {}'.format(statements)
        else:
            return ''

    def setall(self, data):
        for value in data.keys():
            self.set(value, data[value])
        return self

    def set(self, field, value, constant=False):
        if constant:
            self.set_statements.append('{} = {}'.format(field, value))
        else:
            self.set_statements.append('{0} = %({0})s'.format(field))
            self.parameters[field] = value
        return self


class DeleteBuilder(SQLBuilder):

    def sql(self):
        return 'DELETE FROM {} {}'.format(self.table, self.build_where())


class InsertBuilder(SQLBuilder):

    def __init__(self, database, table):
        super(InsertBuilder, self).__init__(database, table)
        self.constants = {}

    def sql(self):
        if len(set(list(self.parameters.keys()) + list(self.constants.keys()))) == len(self.parameters.keys()) + len(self.constants.keys()):
            cols = []
            values = []
            for field in self.parameters:
                cols.append(field)
                values.append('%({})s'.format(field))
            for field in self.constants:
                cols.append(field)
                values.append(self.constants[field])
            return 'INSERT INTO {}({}) VALUES ({})'.format(self.table, ', '.join(cols), ', '.join(values))
        else:
            raise ValueError('There are repeated keys in constants and values')

    def setall(self, data):
        for value in data.keys():
            self.set(value, data[value])
        return self

    def set(self, field, value, constant=False):
        if constant:
            self.constants[field] = value
        else:
            self.parameters[field] = value
        return self


class Page(dict):

    def __init__(self, number, size, data, last_page):
        self["number"] = self.number = number
        self["size"] = self.size = size
        self["data"] = self.data = data
        self["last_page"] = self.last_page = last_page


class Database(object):
    """Facade to access database using psycopg2"""

    def __init__(self, config=None):
        self.config = Config.instance() if config is None else config
        self.connection = self.config.pool.connection()
        self.print_sql = self.config.print_sql

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        if type is None and value is None and tb is None:
            self.connection.commit()
        else:
            self.connection.rollback()
        self.disconnect()

    def __load_query(self, name):
        """Load a query located in ./sql/<name>.sql"""
        try:
            with open(QUERIES_DIR + name + '.sql') as query:
                query_string = query.read()
            return query_string
        except IOError as e:
            if e.errno == errno.ENOENT:
                return name
            else:
                raise e

    def execute(self, name_or_sql, parameters=None, skip_load_query=False):
        """Execute query by name returning cursor"""
        cursor = self.connection.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor)
        if skip_load_query:
            sql = name_or_sql
        else:
            sql = self.__load_query(name_or_sql)
        if self.print_sql:
            print("SQL: {} - PARAMS: {}".format(sql, parameters))

        cursor.execute(sql, parameters)
        return CursorWrapper(cursor)

    def paging(self, name_or_sql, parameters=None, page=0, page_size=10, skip_load_query=True):
        if skip_load_query:
            sql = name_or_sql
        else:
            sql = self.__load_query(name_or_sql)
        sql = '{} LIMIT {} OFFSET {}'.format(
            sql, page_size + 1, page * page_size)
        data = self.execute(sql, parameters, skip_load_query=True).fetchall()
        last_page = len(data) <= page_size
        return Page(page, page_size, data[:-1] if not last_page else data, last_page)

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
