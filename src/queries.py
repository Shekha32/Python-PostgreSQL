
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
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#print one column
def __printcolumn ( db, tablename, column ) -> None:

        query = f"SELECT {column} FROM {tablename};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#print several columns
def __printcolumns ( db, tablename, columns ) -> None:

        query = f"SELECT {','.join ( columns )} FROM {tablename};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#print table order by {field} (ASC/DESC)
def __orderby ( db, tablename, orderby, sort ) -> None:

        query = f"SELECT * FROM {tablename} ORDER BY {orderby} {sort};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#distinct rows in column
def __distinct ( db, tablename, column, sort ) -> None:

        query = f"SELECT DISTINCT {column} FROM {tablename} ORDER BY {column} {sort};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#where + and
def __whereand ( db, tablename, columns ) -> None:

        query = f"SELECT * FROM {tablename} WHERE {columns [ 0 ]}='Female' AND {columns [ 1 ]}='Brazil';"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#where + conditions + order by
def __conditions ( db, tablename, columns ) -> None:

        query = f"SELECT * FROM {tablename} WHERE {columns [ 0 ]}='Female' AND ({columns [ 1 ]}='Poland' OR {columns [ 1 ]}='Germany') ORDER BY {columns [ 2 ]};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#limit
def __limit ( db, tablename, limit ) -> None:

        query = f"SELECT * FROM {tablename} LIMIT {limit};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#offset + limit
def __offset ( db, tablename, offset, limit ) -> None:

        query = f"SELECT * FROM {tablename} OFFSET {offset} LIMIT {limit};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#fetch + limit
def __fetch ( db, tablename, offset, fetch ) -> None:

        query = f"SELECT * FROM {tablename} OFFSET {offset} FETCH FIRST {fetch} ROW ONLY;"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#where + in
def __wherein ( db, tablename, field ) -> None:

        query = f"SELECT * FROM {tablename} WHERE {field} IN ('Canada', 'Peru', 'Israel');"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#where + between
def __wherebetween ( db, tablename, field ) -> None:

        query = f"SELECT * FROM {tablename} WHERE {field} BETWEEN '1990-01-01' AND '1992-01-01';"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#where + like
def __wherelike ( db, tablename, field ) -> None:

        query = f"SELECT * FROM {tablename} WHERE {field} LIKE '%@google.%';"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#various queries
def queries ( db ):

        tablename = 'employee'

        try:
                __droptable ( db, tablename )                                                           #drop table if exists
                __createtable ( db )                                                                    #create table from data (SQL)
                __droptable ( db, tablename )                                                           #drop table if exists
                __createtablecsv ( db, tablename )                                                      #create table from data (CSV->SQL)
                __printtable ( db, tablename )                                                          #print table
                __printcolumn ( db, tablename, column='email' )                                         #print one column
                __printcolumns ( db, tablename, columns=['first_name', 'last_name', 'title'] )          #print several columns
                __orderby ( db, tablename, orderby='city', sort='DESC' )                                #print table order by {field} (ASC/DESC)
                __distinct ( db, tablename, column='country', sort='ASC' )                              #distinct rows in column
                __whereand ( db, tablename, columns=['gender', 'country'] )                             #where + and
                __conditions ( db, tablename, columns=['gender', 'country', 'first_name'] )             #where + conditions + order by
                __limit ( db, tablename, limit=12 )                                                     #limit
                __offset ( db, tablename, offset=200, limit=5 )                                         #offset + limit
                __fetch ( db, tablename, offset=300, fetch=10 )                                         #offset + fetch
                __wherein ( db, tablename, field='country' )                                            #where + in
                __wherebetween ( db, tablename, field='date_of_birth' )                                 #where + between
                __wherelike ( db, tablename, field='email' )                                            #where + like


        except ( Exception, psycopg2.DatabaseError ) as error:
                exit ( error )
