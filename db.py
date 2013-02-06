import psycopg2
import sys

from dj_database_url import config as get_db_config

# Parse the database configuration into something psycopg2 can handle
raw_dbconfig = get_db_config(default='postgres://hurley@localhost/srpush')
dbconfig = {}
if raw_dbconfig['NAME']:
    dbconfig['database'] = raw_dbconfig['NAME']
if raw_dbconfig['USER']:
    dbconfig['user'] = raw_dbconfig['USER']
if raw_dbconfig['PASSWORD']:
    dbconfig['password'] = raw_dbconfig['PASSWORD']
if raw_dbconfig['HOST']:
    dbconfig['host'] = raw_dbconfig['HOST']
if raw_dbconfig['PORT']:
    dbconfig['port'] = raw_dbconfig['PORT']

def get_conn():
    return psycopg2.connect(**dbconfig)

if __name__ == '__main__':
    conn = get_conn()
    cur = conn.cursor()
    if sys.argv[1] == 'create':
        cur.execute("""SELECT * from pg_catalog.pg_tables
                WHERE tablename = 'pushes'""")
        if not cur.fetchall():
            cur.execute("""CREATE TABLE pushes
                    (id serial primary key, srid text, ldap text, sha text,
                     handled boolean default 'f')""")

        cur.execute("""SELECT * FROM pg_catalog.pg_tables
                WHERE tablename = 'netconfigs'""")
        if not cur.fetchall():
            cur.execute("""CREATE TABLE netconfigs
                    (id serial primary key, name text)""")
            cur.execute("""INSERT INTO netconfigs (name) VALUES
                    ('broadband')""")
            cur.execute("""INSERT INTO netconfigs (name) VALUES ('umts')""")
            cur.execute("""INSERT INTO netconfigs (name) VALUES ('gsm')""")

        cur.execute("""SELECT * FROM pg_catalog.pg_tables
                WHERE tablename = 'operating_systems'""")
        if not cur.fetchall():
            cur.execute("""CREATE TABLE operating_systems
                    (id serial primary key, name text)""")
            cur.execute("""INSERT INTO operating_systems (name) VALUES
                    ('windows')""")
            cur.execute("""INSERT INTO operating_systems (name) VALUES
                    ('mac')""")
            cur.execute("""INSERT INTO operating_systems (name) VALUES
                    ('linux')""")

        cur.execute("""SELECT * FROM pg_catalog.pg_tables
                WHERE tablename = 'push_netconfigs'""")
        if not cur.fetchall():
            cur.execute("""CREATE TABLE push_netconfigs
                    (pushid int REFERENCES pushes(id) ON DELETE CASCADE,
                     ncid int REFERENCES netconfigs(id) ON DELETE CASCADE)""")

        cur.execute("""SELECT * FROM pg_catalog.pg_tables
                WHERE tablename = 'push_operating_systems'""")
        if not cur.fetchall():
            cur.execute("""CREATE TABLE push_operating_systems
                    (pushid int REFERENCES pushes(id) ON DELETE CASCADE,
                     osid int REFERENCES operating_systems(id) ON DELETE CASCADE)""")

    conn.commit()
    conn.close()
