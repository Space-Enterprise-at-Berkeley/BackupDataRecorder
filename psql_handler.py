import psycopg2
from psycopg2 import extras
from config import config
import os


def create_new_database(database_name):
    print(f"Creating new database: \"{database_name}\"")
    print("="*10)
    
    print(f"psql -c \"CREATE DATABASE {database_name} ;\"")
    
    os.system(f"psql -c \"CREATE DATABASE {database_name} ;\"")
    
    print("="*10)
    print(f"Created new database: \"{database_name}\"")


def database_exists(params):
    print("Checking if database \"{0}\" exists".format(params["database"]))
    
    con_params = params.copy()
    del con_params["database"]
    
    connection = create_connection(con_params)
    
    if connection is not None:
        cursor = connection.cursor()
        cursor.execute("SELECT datname FROM pg_database")
        db_list = cursor.fetchall()
        
        if (params["database"],) in db_list:
            print("Database \"{0}\" exists".format(params["database"]))
            return True
        else:
            print("Database \"{0}\" does not exist".format(params["database"]))
            return False


def load_database(params):
    if not database_exists(params):
        create_new_database(params["database"])
    return create_connection(params)


def table_exists(connection, table_name):
    print(f"Checking if table \"{table_name}\" exists")
    
    cursor = connection.cursor()
    cursor.execute(f"""SELECT COUNT(*) = 1 FROM pg_tables WHERE tablename = '{table_name}';""")
    exists = cursor.fetchone()[0]
    
    if exists:
        print(f"Table \"{table_name}\" exists")
        return True
    else:
        print(f"Table \"{table_name}\" does not exist")
        return False


def create_new_table(connection, database_config):
    print("Creating new table")
    print("="*10)
    
    cursor = connection.cursor()
    
    command = """CREATE TABLE {0}\n(\n""".format(database_config["table_name"])
    for i, d in enumerate(zip(database_config["columns"], database_config["data_type"])):
        if i == 0:
            command = command + """{0} {1} PRIMARY KEY NOT NULL""".format(d[0], d[1])
        else:
            command = command + """{0} {1} NOT NULL""".format(d[0], d[1])
        if i != len(database_config["columns"]) - 1:
            command = command + ""","""
        command = command + """\n"""        
    command = command + """)"""
    print(command)
    
    cursor.execute(command)
    connection.commit()
    
    print("="*10)
    print("Table created")


def load_table(connection, database_config):
    if not table_exists(connection, database_config["table_name"]):
        create_new_table(connection, database_config)


def save_data(connection, table_name, column_names, data):
    cursor = connection.cursor()
    
    command = f"""INSERT INTO {table_name} ("""
    for c in column_names:
        command = command + f"""{c}, """
    command = command[:-2] + """)\nVALUES ("""
    # command = (command + """%s, """ * len(column_names))[:-2] + """)"""
    command = command[:-1] + """%s"""
    
    extras.execute_values(cursor, command, data)
    connection.commit()
    
        
def create_connection(params):
    try:
        conn = psycopg2.connect(**params)
        print(f"Connected using config: {params}")
        return conn
    except:
        print(f"Failed connecting using config: {params}")


