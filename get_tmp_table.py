"""This service allows to get data from tmp table"""
import os
import sys
import time
import MySQLdb
from rq import Worker, Queue, Connection
from methods.connection import get_redis, get_cursor

r = get_redis()

def get_tmp_table(name, type, col=False, value=False):
    """Returns table info from databse"""
    cursor, _ = get_cursor()
    if not cursor:
        # log that failed getting cursor
        return False
    if "tmp" not in name:
        # log that name was wrong
        return False
    q = f'''SELECT * FROM `{name}` '''
    if type is not None:
        if value:
            value = value.replace(";", "")
            value = value.replace("'", "''")
        if type == "WHERE" and col and value:
            q += f'''WHERE {col} = "{value}"'''
        elif type == "*":
            pass
        elif type == "data":
            q = f'''SELECT data FROM `{name}` '''
        else:
            return False
    try:
        cursor.execute(q)
    except MySQLdb.Error as error:
        print(error)
        # Log
        return False
        # sys.exit("Error:Failed getting new channels from database")
    data = cursor.fetchall()
    cursor.close()
    return data


if __name__ == '__main__':
    q = Queue('get_tmp_table', connection=r)
    with Connection(r):
        worker = Worker([q], connection=r,  name='get_tmp_table')
        worker.work()
