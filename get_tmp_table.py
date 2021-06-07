"""This service allows to get data from tmp table"""
import os
import sys
import time
import MySQLdb
from rq import Worker, Queue, Connection
from methods.connection import get_redis, get_cursor


def get_tmp_table(type=None, col=None, value=None):
    """Returns table info from databse"""
    cursor, _ = get_cursor()
    q = '''SELECT * FROM channels '''
    if type is not None:
        value = value.replace(";", "")
        value = value.replace("'", "''")
        if type == "WHERE":
            value
            q += f'''WHERE {col} = "{value}"'''
    try:
        cursor.execute(q)
    except MySQLdb.Error as error:
        print(error)
        # sys.exit("Error:Failed getting new channels from database")
    data = cursor.fetchall()
    cursor.close()
    return data


if __name__ == '__main__':
    time.sleep(5)
    r = get_redis()
    q = Queue('get_tmp_table', connection=r)
    with Connection(r):
        worker = Worker([q], connection=r,  name='get_tmp_table')
        worker.work()
