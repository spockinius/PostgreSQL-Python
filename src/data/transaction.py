from configparser import ConfigParser
import psycopg2



def config(filename="./database.ini", section="postgresql"):
    parser = ConfigParser()
    parser.read(filename)
    db= {}

    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]

    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    
    return db

def connect():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = 'SELECT *FROM person;'
        cursor.execute(SQL)
        row = cursor.fetchone()
        print(row)
        cursor.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if con is not None:
            con.close()

conn = None

def transaction(tablename):
    try:
        # Connect to an existing database
        conn = psycopg2.connect(**config())

        # Open a cursor to perform database operations
        cur = conn.cursor()

        # Execute a command: this creates a new table
        cur.execute(f"CREATE TABLE {tablename} (id serial PRIMARY KEY, num integer, data varchar);")
        print("Table created")
        # Pass data to fill a query placeholders and let Psycopg perform
        # the correct conversion (no more SQL injections!)
        cur.execute(f"INSERT INTO {tablename} (num, data) VALUES (%s, %s)",
        (100, "abc'def"))
        print("values added to table")

        # Query the database and obtain data as Python objects
        cur.execute(f"SELECT * FROM {tablename};")
        cur.fetchone()
        (1, 100, "abc'def")
        print("Values selected from table")
        
        SQL = f"SELECT * FROM {tablename};"
        cur.execute(SQL)
        row = cur.fetchall()
        print(row)
        # Make the changes to the database persistent
        conn.commit()

        # Close communication with the database
        cur.close()
        conn.close()

    except psycopg2.DatabaseError as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()

def bank():
    conn = psycopg2.connect(**config())
    try:
        cur = conn.cursor()
        amount = 2500

        query = 'SELECT balance FROM bank01 WHERE id = 1;'
        cur.execute(query)
        record = cur.fetchone()[0]
        balance_account_A = int(record)
        balance_account_A -= amount

        # Withdraw from account A  now
        sql_update_query = 'UPDATE bank01 SET balance = %s WHERE id = 1;'
        cur.execute(sql_update_query, (balance_account_A,))

        query = 'SELECT balance FROM bank01 WHERE id = 2;'
        cur.execute(query)
        record = cur.fetchone()[0]
        balance_account_B = int(record)
        balance_account_B += amount

        # Credit to  account B  now
        sql_update_query = 'UPDATE bank01 SET balance = %s WHERE id = 2;'
        cur.execute(sql_update_query, (balance_account_B,))

        # commiting both the transction to database
        conn.commit()
        print("Transaction completed successfully ")

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error in transction Reverting all other operations of a transction ", error)
        conn.rollback()

    finally:
        # closing database connection.
        if conn:
            cur.close()
            conn.close()
            print("PostgreSQL connection is closed")

if __name__ == "__main__":
    bank()