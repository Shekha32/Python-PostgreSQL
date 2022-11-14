
#Execution of various queries to the database (PostgreSQL)

#system libraries
import psycopg2
from pprint import pprint


#drop table
def __droptable ( db, tablename ) -> None:

        query = f"DROP TABLE IF EXISTS {tablename};"
        db.query ( query )


#create table from data (SQL)
def __createtable ( db ) -> None:

        query = open ( './data/employee.sql', 'r' ).read()
        db.query ( query )


#create table from data (CSV->SQL)
def __createtablecsv ( db, tablename ) -> None:

        head = f"CREATE TABLE {tablename} ( " + \
                "id BIGSERIAL NOT NULL PRIMARY KEY, " + \
                "first_name VARCHAR(50) NOT NULL, " + \
                "last_name VARCHAR(50) NOT NULL, " + \
                "date_of_birth DATE NOT NULL, " + \
                "gender VARCHAR(30) NOT NULL, " + \
                "city VARCHAR(40) NOT NULL, " + \
                "country VARCHAR(40) NOT NULL, " + \
                "title VARCHAR(50) NOT NULL, " + \
                "email VARCHAR(100) );"

        with open ( './data/employee.csv', 'r' ) as f:
                lines = ""
                fields = f.readline().rstrip ( '\n' )                   #read first line

                while True:
                        line = f.readline().rstrip ( '\n' )

                        if not line:
                                break

                        line = line.split(',')                          #add quotes to all fields of each line
                        line[1:] = [ "'" + field + "'" if field != '' and line.index ( field ) != 0 else 'null' for field in line[1:] ]
                        line = ",".join ( line )
                        lines += f"insert into {tablename} ({fields}) values ({line});"

        query = head + lines
        db.query ( query )


#print table
def __printtable ( db, tablename ) -> None:

        query = f"SELECT * FROM {tablename};"
        pprint ( db.query ( query, selection=True ), width=200 )


#print one column
def __printcolumn ( db, tablename, column ) -> None:

        query = f"SELECT {column} FROM {tablename};"
        pprint ( db.query ( query, selection=True ), width=200 )


#print several columns
def __printcolumns ( db, tablename, columns ) -> None:

        query = f"SELECT {','.join ( columns )} FROM {tablename};"
        pprint ( db.query ( query, selection=True ), width=200 )


#print table order by {field} (ASC/DESC)
def __orderby ( db, tablename, orderby, mode ) -> None:

        query = f"SELECT * FROM {tablename} ORDER BY {orderby} {mode};"
        pprint ( db.query ( query, selection=True ), width=200 )


#various queries
def queries ( db ):

        tablename = 'employee'

        try:
                __droptable ( db, tablename )                                                           #drop table if exists
                __createtable ( db )                                                                    #create table from data (SQL)
                __droptable ( db, tablename )                                                           #drop table if exists
                __createtablecsv ( db, tablename )                                                      #create table from data (CSV->SQL)
                #__printtable ( db, tablename )                                                         #print table
                __printcolumn ( db, tablename, column='email' )                                         #print one column
                __printcolumns ( db, tablename, columns=['first_name', 'last_name', 'title'] )          #print several columns
                __orderby ( db, tablename, orderby='city', mode='DESC' )                                #print table order by {field} (ASC/DESC)


        except ( Exception, psycopg2.DatabaseError ) as error:
                exit ( error )
