
#Execution of various queries to the database (PostgreSQL)

#system libraries
import pprint
import psycopg2


#drop table
def __droptable ( db, tablename ) -> None:

        query = f"DROP TABLE IF EXISTS {tablename};"
        db.query ( query )


#create table (SQL)
def __createtable ( db ) -> None:

        query = open ( './data/employee.sql', 'r' ).read()
        db.query ( query )


#create table (CSV->SQL)
def __createtablecsv ( db, tablename ) -> None:

        pass
        #db.query ( query )


#various queries
def queries ( db ):

        tablename = 'employee'

        try:
                __droptable ( db, tablename )           #drop table if exists
                __createtable ( db )                    #create table from data (SQL)
                #__createtablecsv ( db, tablename )     #create table from data (CSV->SQL)

        except ( Exception, psycopg2.DatabaseError ) as error:
                exit ( error )

        else:
                print ( '\n\nAll queries have been successfully executed!' )
