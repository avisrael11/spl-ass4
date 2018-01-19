
import sys
import os
import sqlite3
import atexit

# connect to the database
_conn = sqlite3.connect('world.db')

counter_task_id = 1


# register a function to be called immediately when the interpreter terminates
def _close_db():
    _conn.commit()
    _conn.close()


atexit.register(_close_db)


# our application API:

def create_tables():
    _conn.executescript("""
        CREATE TABLE resources (
            name      TEXT        PRIMARY KEY,
            amount    INTEGER        NOT NULL
        );

        CREATE TABLE workers (
            id        INTEGER   PRIMARY KEY,
            name      TEXT      NOT NULL,
            status    TEXT      NOT NULL
        );

        CREATE TABLE tasks (
            id              INTEGER     PRIMARY KEY,
            task_name       TEXT        NOT NULL,
            worker_id       INTEGER     NOT NULL,
            time_to_make    INTEGER     NOT NULL,
            resource_name   TEXT        NOT NULL,
            resource_amount INTEGER     NOT NULL,
            FOREIGN KEY(worker_id)     REFERENCES workers(id)

            
        );
    """)


def insert_resource(name, amount):
    _conn.execute("""
        INSERT INTO resources (name, amount) VALUES (?, ?)
    """, [name, amount])


def insert_worker(id, name):
    _conn.execute("""
        INSERT INTO workers (id, name, status) VALUES (?, ?, ?)
    """, [id, name, "idle"])


def insert_tasks(task_name, worker_id, time_to_make, resource_name, resource_amount):
    global counter_task_id
    _conn.execute("""
        INSERT INTO tasks (id, task_name, worker_id, time_to_make, resource_name, resource_amount) VALUES (?, ?, ?, ?, ?, ?)
    """, [counter_task_id, task_name, worker_id, time_to_make, resource_name, resource_amount])
    counter_task_id += 1


def print_all_db():
    cur = _conn.cursor()

    cur.execute("""
        SELECT *
        FROM tasks
    """)
    print (cur.fetchall())




create_tables()


# Check if get file
try:
    sys.argv[1]
except IndexError:
    print ("Error - missing config file name!")
    sys.exit(1)
else:
    configFile = sys.argv[1]

# Check on whether file exists and is accessible
if not os.path.isfile(configFile):
    print ("Error - input file", configFile, "is not accessible!")
    sys.exit(1)
else:
    f = open(configFile, "r")




for line in f:
   listPerLine = line.split(",")
   if len(listPerLine) == 2: #if resource
           insert_resource(listPerLine[0], listPerLine[1])

   elif len(listPerLine) == 3:  # if worker
       insert_worker(listPerLine[1], listPerLine[2])

   elif len(listPerLine) == 5:  # if task
       insert_tasks(listPerLine[0], listPerLine[1], listPerLine[4], listPerLine[2], listPerLine[3])
   else:
       print ("The input unidentify")

#print_all_db()