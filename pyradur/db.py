# MIT License
#
# Copyright (c) 2018-2019 Garmin International or its subsidiaries
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import sqlite3
import logging
import json

logger = logging.getLogger('pyradur.db')

class DBManager(object):
    def __init__(self, logger=logger):
        self.dbs = {}
        self.db_factory = None
        self.logger = logger

    def set_db_factory(self, factory):
        self.db_factory = factory

    def add_db(self, name, db):
        self.logger.debug("Added database '%s'", name)
        self.dbs[name] = db

    def get_db(self, name):
        try:
            return self.dbs[name]
        except KeyError:
            if self.db_factory:
                self.add_db(name, self.db_factory(name))
                return self.dbs[name]
            self.logger.debug("Database '%s' not found", name)
            raise

class Sqlite3DB(object):
    def __init__(self, db_path, *args, **kwargs):
        self.db_path = db_path

        self.db = sqlite3.connect(db_path, *args, **kwargs)
        self.db.text_factory = str

        self.cursor = self.db.cursor()

        self.cursor.execute("pragma journal_mode = WAL;")
        self.cursor.execute("CREATE TABLE IF NOT EXISTS data(key TEXT PRIMARY KEY NOT NULL, value TEXT);")
        self.db.commit()

    def __getitem__(self, key):
        logger.debug('Getting %s', key)

        self.cursor.execute("SELECT * from data where key=?;", [key])
        row = self.cursor.fetchone()
        logger.debug('%s = %s', key, row)
        if row is not None:
            return json.loads(row[1])
        raise KeyError

    def __setitem__(self, key, value):
        logger.debug('Setting %s = %s', key, value)

        self.cursor.execute("SELECT * from data where key=?;", [key])
        row = self.cursor.fetchone()
        if row is not None:
            self.cursor.execute("UPDATE data SET value=? WHERE key=?;", [json.dumps(value), key])
        else:
            self.cursor.execute("INSERT into data(key, value) values (?, ?);", [key, json.dumps(value)])
        self.db.commit()

    def __delitem__(self, key):
        logger.debug('Deleting %s', key)
        self.cursor.execute("DELETE from data where key=?;", [key])
        self.db.commit()


