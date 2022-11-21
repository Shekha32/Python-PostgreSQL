
#MAIN

#system libraries
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

#project modules
import queries


#database operations
class Database():

        #constructor
        def __init__ ( self ) -> None:
                pass


        #destructor - disconnect from database
        def __del__ ( self ) -> None:

                if self.connection:
                        self.cursor.close()
                        self.connection.close()


        #database connection
        def dbconn ( self ) -> None:

                self.connection = psycopg2.connect (
                        database="mydb", 
                        user="postgres", 
                        password="3232", 
                        host="localhost", 
                        port="5432"
                )
                self.connection.set_isolation_level ( ISOLATION_LEVEL_AUTOCOMMIT )
                self.cursor = self.connection.cursor()
                #print ( "Database opened successfully" )


        #execution of query to the database
        def query ( self, query, selection=False ):

                self.cursor.execute ( query )

                if selection:
                        return self.cursor.fetchall()


        #creation of csv files (export SQL tables to CSV)
        def create_csv ( self, query, file ):

                self.cursor.copy_expert ( query, file )


#main
def __main() -> None:

        qs = Database()                         #class object creation

        try:
                qs.dbconn()                     #database connection

        except ( Exception, psycopg2.DatabaseError ) as error:
                exit ( error )

        else:
                queries.queries ( qs )          #execution of various queries to the database (PostgreSQL)
                queries.create_csv ( qs )       #creation of csv files (export SQL tables to CSV)

        print ( '\n\nENDMAIN: All queries successfully executed!' )


if __name__ == '__main__':
        __main()
