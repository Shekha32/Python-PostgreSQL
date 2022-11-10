
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


#create table from data (CSV->SQL)                      #TODO
def __createtablecsv ( db, tablename ) -> None:

        pass
        #db.query ( query )


#print table
def __printtable ( db, tablename ) -> None:

        query = f"SELECT * FROM {tablename};"
        pprint ( db.query ( query, selection=True ), width=200 )


#various queries
def queries ( db ):

        tablename = 'employee'

        try:
                __droptable ( db, tablename )           #drop table if exists
                __createtable ( db )                    #create table from data (SQL)
                #__createtablecsv ( db, tablename )     #create table from data (CSV->SQL)
                __printtable ( db, tablename )          #print table

        except ( Exception, psycopg2.DatabaseError ) as error:
                exit ( error )
