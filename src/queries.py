
#Execution of various queries to the database (PostgreSQL)

#system libraries
import pprint
import psycopg2


#drop table
def __droptable ( db ) -> None:
        query = "DROP TABLE IF EXISTS employee;"
        db.query ( query )


#create table (SQL)
def __createtable ( db ) -> None:

        with open ( './data/employee.sql', 'r' ) as file:
                query = file.readlines()
                
        db.query ( query )


#TODO: create table (CSV->SQL)
#...


def queries ( db ):

        try:
                pass
                #__droptable ( db )
                #__createtable ( db )

        except ( Exception, psycopg2.DatabaseError ) as error:
                exit ( error )

        else:
                print ( '\n\nAll queries have been successfully executed!' )
