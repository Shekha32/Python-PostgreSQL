
#MAIN

#system libraries
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

#project modules
import queries


#database operations
class Database():

        #constructor
        def __init__( self ) -> None:
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

                #print ( 'query: ', query )

                self.cursor.execute ( query )

                if selection:
                        return self.cursor.fetchall()


#main
def __main() -> None:

        qs = Database()         #class object creation

        try:
                qs.dbconn()     #database connection

        except ( Exception, psycopg2.DatabaseError ) as error:
                exit ( error )

        else:
                queries.queries ( qs )
        
        print ( '\n\nENDMAIN: All queries have been successfully executed!' )


if __name__ == '__main__':
        __main()
