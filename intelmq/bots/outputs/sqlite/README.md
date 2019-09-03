# How to install:

Since this bot is based on PostgreSQL, you can use `intelmq_psql_initdb` to create initial sql-statements
from Harmonization.conf. The script will create the required table layout
and save it as /tmp/initdb.sql

Create the new database (you can ignore all errors since SQLite doesn't know all SQL features generated for PostgreSQL):

```bash
sqlite3 your-db.db
sqlite> .read /tmp/initdb.sql
```