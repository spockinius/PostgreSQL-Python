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

def getallperson():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = 'SELECT * FROM person;'
        cursor.execute(SQL)
        row = cursor.fetchall()
        print(row)
        cursor.close()
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    
    finally:
        if con is not None:
            con.close()



def getcolumnsperson():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'person' ORDER BY ORDINAL_POSITION;"
        cursor.execute(SQL)
        row = cursor.fetchall()
        print(row)
        cursor.close()
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    
    finally:
        if con is not None:
            con.close()

def getcertificatescolumns():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'certificates' ORDER BY ORDINAL_POSITION;"
        cursor.execute(SQL)
        row = cursor.fetchall()
        print(row)
        cursor.close()
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    
    finally:
        if con is not None:
            con.close()

def getcertificates():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = 'SELECT * FROM certificates;'
        cursor.execute(SQL)
        row = cursor.fetchall()
        print(row)
        cursor.close()
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    
    finally:
        if con is not None:
            con.close()

def addrowcertificates(nimi):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = "INSERT INTO certificates (name) VALUES (%s);"
        cursor.execute(SQL, (nimi,))
        con.commit()
        print("New row added")
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    
    finally:
        if con is not None:
            con.close()

def updatepersonrow(age, name):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = "UPDATE person SET age = (%s) WHERE name = (%s);"
        cursor.execute(SQL, (age, name,))
        con.commit()
        print("Row updated")
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    
    finally:
        if con is not None:
            con.close()

def updatecertificaterow(name, personid):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = "UPDATE certificates SET name = (%s) WHERE person_id = (%s);"
        cursor.execute(SQL, (name, personid))
        con.commit()
        print("Row updated")
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    
    finally:
        if con is not None:
            con.close()


def deletepersonrow(name):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = "DELETE FROM person WHERE name = (%s);"
        cursor.execute(SQL, (name,))
        con.commit()
        print("Row updated")
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    
    finally:
        if con is not None:
            con.close()

# EI TOIMI VIELÄ
def create_tables(name, game):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = "CREATE TABLE %s (id SERIAL PRIMARY KEY, %s VARCHAR(255) NOT NULL;"
        cursor.execute(SQL, (name, game,))
        con.commit()
        print("Table created")
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    
    finally:
        if con is not None:
            con.close()


while True:
    syote = input("Choose a function: " + "\n" +
    "1=getallperson, 2=getcolumnsperson, 3=getcertificates, 4=getcertificatescolumns: ")
    if syote == "1":
        getallperson()
        jatka = input("Continue? Y/N: ")
        if jatka == "Y":
            continue
        else:
            break
    elif syote == "2":
        getcolumnsperson()
        jatka = input("Continue? Y/N: ")
        if jatka == "Y":
            continue
        else:
            break
    
    elif syote == "3":
        getcertificates()
        jatka = input("Continue? Y/N: ")
        if jatka == "Y":
            continue
        else:
            break

    else:
        getcertificatescolumns()
        jatka = input("Continue? Y/N: ")
        if jatka == "Y":
            continue
        else:
            break
