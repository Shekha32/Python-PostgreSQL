
#MAIN

import psycopg2


class DBconn():

        #конструктор
        def __init__( self ) -> None:

                print ( '\ninit\n' )
                pass


        #деструктор
        def __del__ ( self ) -> None:

                print ( '\ndel\n' )

                if self.connection:
                        self.cursor.close()
                        self.connection.close()

        #подключение к БД
        def dbconn ( self ) -> None:

                self.connection = psycopg2.connect (
                        database="mydb", 
                        user="postgres", 
                        password="3232", 
                        host="127.0.0.1", 
                        port="5432"
                )

                self.cursor = self.connection.cursor()

                print ( "Database opened successfully" )


def __main() -> None:

        print ( 'main' )

        pp = DBconn()

        try:
                pp.dbconn()

        except ( Exception, psycopg2.DatabaseError ) as error:
                print ( error )

        else:
                pass


if __name__ == '__main__':
        __main()
