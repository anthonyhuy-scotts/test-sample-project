import os
import sys
import argparse
import logging
import unicodecsv as csv
import psycopg2
from datetime import datetime, timedelta
import time
from psycopg2.extras import NamedTupleCursor
import urllib.parse as urlparse


class PostgresConn:

    def __init__(self, connect_string):
        self.connect_string = connect_string
        self.conn = None

    def connect(self):

        split = urlparse.urlparse(self.connect_string)
        conn_string = 'host={} dbname={} user={} password={}'.format(
            split.hostname, split.path.split('/')[1], split.username, split.password)
        try:
            self.conn = psycopg2.connect(conn_string)
            if self.conn is None:
                print("Unable to connect {}".format(split.path.split('/')[1]))
                return False
        except psycopg2.OperationalError as e:
            print("Unexpected exception while connecting to postgres: {}".format(e))
            False
        return True

    def run_query(self, query_string_file):
        with open(query_string_file, 'r') as f:
            query_string = f.read()
        with self.conn.cursor(cursor_factory=NamedTupleCursor) as cursor:
            cursor.execute(query_string)
            query_records = cursor.fetchall()
        return query_records

