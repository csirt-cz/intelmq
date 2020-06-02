# -*- coding: utf-8 -*-
"""
SQLLite output bot.

See Readme.md for installation and configuration.

In case of errors, the bot tries to reconnect if the error is of operational
and thus temporary. We don't want to catch too much, like programming errors
(missing fields etc).
"""

from intelmq.lib.bot import Bot

try:
    import sqlite3
except ImportError:
    sqlite3 = None


class SQLiteOutputBot(Bot):

    def init(self):
        self.logger.debug("Connecting to SQLite3.")
        if sqlite3 is None:
            raise ValueError("Could not import 'sqlite3'. Please install it.")

        try:
            self.con = sqlite3.connect("/tmp/ram/db.db") # XXXself.parameters.file)
            self.cur = self.con.cursor()
            self.table = "events"  # XXX self.parameters.table
            self.jsondict_as_string = getattr(self.parameters, 'jsondict_as_string', True)
        except (sqlite3.Error, Exception) as e:
            self.logger.exception('Failed to connect to database: %e', e)
            raise
        self.logger.info("Connected to SQLite3.")

    def process(self):
        event = self.receive_message().to_dict(jsondict_as_string=self.jsondict_as_string)

        keys = '", "'.join(event.keys())
        values = list(event.values())
        fvalues = len(values) * '?, '
        query = ('INSERT INTO {table} ("{keys}") VALUES ({values})'
                 ''.format(table=self.table, keys=keys, values=fvalues[:-2]))

        self.logger.debug('Query: %r with values %r.', query, values)

        try:
            # note: this assumes, the DB was created with UTF-8 support!
            self.cur.execute(query, values)
        except (sqlite3.InterfaceError, sqlite3.InternalError,
                sqlite3.OperationalError, AttributeError):
            try:
                self.con.rollback()
                self.logger.exception('Executed rollback command '
                                      'after failed query execution.')
            except sqlite3.OperationalError:
                self.logger.exception('Executed rollback command '
                                      'after failed query execution.')
                self.init()
            except Exception:
                self.logger.exception('Cursor has been closed, connecting '
                                      'again.')
                self.init()
        else:
            self.con.commit()
            self.acknowledge_message()


BOT = SQLiteOutputBot
