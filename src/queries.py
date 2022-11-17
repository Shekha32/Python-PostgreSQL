
#Execution of various queries to the database (PostgreSQL)

#system libraries
import psycopg2
from pprint import pprint


#DROP TABLE IF EXISTS
def __drop_table ( db, tablename ) -> None:

        query = f"DROP TABLE IF EXISTS {tablename};"
        db.query ( query )


#CREATE TABLE FROM data (SQL)
def __create_table ( db, tablename ) -> None:

        query = open ( f'./data/{tablename}.sql', 'r' ).read()
        db.query ( query )


#CREATE TABLE FROM data (CSV->SQL)
def __create_table_csv ( db, tablename ) -> None:

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


#DELETE row
def __delete_row ( db, tablename, id ) -> None:

        query = f"DELETE FROM {tablename} WHERE id={id};"
        print ( '\n', query )
        db.query ( query )


#ALTER TABLE - remove PRIMARY KEY
def __remove_primary_key ( db, tablename ) -> None:

        query = f"ALTER TABLE {tablename} DROP CONSTRAINT {tablename}_pkey;"
        print ( '\n', query )
        db.query ( query )


#ADD PRIMARY KEY
def __add_primary_key ( db, tablename, field ) -> None:

        query = f"ALTER TABLE {tablename} ADD PRIMARY KEY({field});"
        print ( '\n', query )
        db.query ( query )


#ALTER TABLE - ADD unique field
def __unique_field ( db, tablename, field ) -> None:

        query = f"ALTER TABLE {tablename} ADD CONSTRAINT unique_{field} UNIQUE({field});"
        print ( '\n', query )
        db.query ( query )


#print table
def __print_table ( db, tablename ) -> None:

        query = f"SELECT * FROM {tablename};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#print table column names
def __print_columns ( db, tablename ) -> None:

        query = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{tablename}';"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#print one column
def __column ( db, tablename, column ) -> None:

        query = f"SELECT {column} FROM {tablename};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#print several columns
def __columns ( db, tablename, columns ) -> None:

        query = f"SELECT {','.join ( columns )} FROM {tablename};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#print table ORDER BY {field} (ASC/DESC)
def __orderby ( db, tablename, orderby, sort ) -> None:

        query = f"SELECT * FROM {tablename} ORDER BY {orderby} {sort};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#DISTINCT rows in column
def __distinct ( db, tablename, column, sort ) -> None:

        query = f"SELECT DISTINCT {column} FROM {tablename} ORDER BY {column} {sort};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#WHERE + AND
def __where_and ( db, tablename, columns ) -> None:

        query = f"SELECT * FROM {tablename} WHERE {columns [ 0 ]}='Female' AND {columns [ 1 ]}='Brazil';"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#WHERE + conditions + ORDER BY
def __conditions ( db, tablename, columns ) -> None:

        query = f"SELECT * FROM {tablename} WHERE {columns [ 0 ]}='Female' AND ({columns [ 1 ]}='Poland' OR {columns [ 1 ]}='Germany') ORDER BY {columns [ 2 ]};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#LIMIT | note: print only {limit} rows
def __limit ( db, tablename, limit ) -> None:

        query = f"SELECT * FROM {tablename} LIMIT {limit};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#OFFSET + LIMIT | note: print only {limit} rows starting from {offset}
def __offset ( db, tablename, offset, limit ) -> None:

        query = f"SELECT * FROM {tablename} OFFSET {offset} LIMIT {limit};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#OFFSET + FETCH | note: print only {limit} rows starting from {offset}
def __fetch ( db, tablename, offset, fetch ) -> None:

        query = f"SELECT * FROM {tablename} OFFSET {offset} FETCH FIRST {fetch} ROW ONLY;"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#WHERE + IN | note: employees from a particular country
def __where_in ( db, tablename, field ) -> None:

        query = f"SELECT * FROM {tablename} WHERE {field} IN ('Canada', 'Peru', 'Israel');"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#WHERE + BETWEEN | note: employees born on a certain date
def __where_between ( db, tablename, field ) -> None:

        query = f"SELECT * FROM {tablename} WHERE {field} BETWEEN '1990-01-01' AND '1992-01-01';"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#WHERE + LIKE | note: employees with specific {field} format
def __where_like ( db, tablename, field ) -> None:

        query = f"SELECT * FROM {tablename} WHERE {field} LIKE '%@google.%';"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#COUNT + GROUP BY | note: amount of employees from counties
def __count_groupby ( db, tablename, field ) -> None:

        query = f"SELECT {field}, COUNT(*) FROM {tablename} GROUP BY {field};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#COUNT + GROUP BY + HAVING + ORDER BY | note: amount of employees(>{amount}) from counties
def __count_groupby_having ( db, tablename, field, amount ) -> None:

        query = f"SELECT {field}, COUNT(*) FROM {tablename} GROUP BY {field} HAVING COUNT(*) > {amount} ORDER BY COUNT(*);"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#AS | note: change headers
def __as ( db, tablename, fields ) -> None:

        query = f"SELECT id, {fields [ 0 ]} AS name, {fields [ 1 ]} AS surname, country, {fields [ 2 ]} AS sex FROM employee;"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )  


#COALESCE + ORDER BY | note: replace null by {msg}
def __coalesce ( db, tablename, field, msg ) -> None:

        query = f"SELECT COALESCE({field}, '{msg}') FROM employee ORDER BY email;"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 ) 


#AGE + NOW + AS | note: age of employees
def __age_now_as ( db, tablename, field ) -> None:

        query = f"SELECT first_name, last_name, title, AGE(NOW(), {field}) AS age FROM {tablename};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 ) 


#UPDATE + SET + WHERE | note: update {id} row
def __update_set ( db, tablename, id ) -> None:

        query = f"UPDATE {tablename} SET first_name = 'Alex', last_name = 'Shekha', email = 'le.shekha@mail.com' WHERE id = {id};"
        print ( '\n', query )
        pprint ( db.query ( query ), width=200 ) 


#MIN, MAX | note: MIN/MAX price of tickets
def __min_max ( db, tablename, field, operation ) -> None:

        query = f"SELECT {operation}({field}) FROM {tablename};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 ) 


#ROUND + AVERAGE | note: round {field}
def __round_avg ( db, tablename, field ) -> None:

        query = f"SELECT ROUND(AVG({field})) FROM {tablename};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 ) 


#MIN + GROUP BY | note: most cheap tickets in countries
def __min_groupby ( db, tablename, fields ) -> None:

        query = f"SELECT {fields [ 0 ]}, MIN({fields [ 1 ]}) FROM {tablename} GROUP BY {fields [ 0 ]};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 ) 


#SUM + GROUP BY | note: amount of tickets to countries
def __sum_groupby ( db, tablename, fields ) -> None:

        query = f"SELECT {fields [ 0 ]}, SUM({fields [ 1 ]}) FROM {tablename} GROUP BY {fields [ 0 ]};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 ) 


#various queries
def queries ( db ):

        tablename = 'employee'

        try:
                __drop_table ( db, tablename )                                                  #DROP TABLE IF EXISTS
                __create_table ( db, tablename )                                                #CREATE TABLE FROM data (SQL)
                __drop_table ( db, tablename )                                                  #DROP TABLE IF EXISTS
                __create_table_csv ( db, tablename )                                            #CREATE TABLE FROM data (CSV->SQL)
                __delete_row ( db, tablename, id='500' )                                        #DELETE row
                __remove_primary_key ( db, tablename )                                          #ALTER TABLE - remove PRIMARY KEY
                __add_primary_key ( db, tablename, field='id' )                                 #ADD PRIMARY KEY
                __unique_field ( db, tablename, field='email' )                                 #ALTER TABLE - ADD unique field
                __print_table ( db, tablename )                                                 #print table
                __print_columns ( db, tablename )                                               #print table column names
                __column ( db, tablename, column='email' )                                      #print one column
                __columns ( db, tablename, columns=['first_name', 'last_name', 'title'] )       #print several columns
                __orderby ( db, tablename, orderby='city', sort='DESC' )                        #print table ORDER BY {field} (ASC/DESC)
                __distinct ( db, tablename, column='country', sort='ASC' )                      #DISTINCT rows in column
                __where_and ( db, tablename, columns=['gender', 'country'] )                    #WHERE + AND
                __conditions ( db, tablename, columns=['gender', 'country', 'first_name'] )     #WHERE + conditions + ORDER BY
                __limit ( db, tablename, limit=12 )                                             #LIMIT | note: print only {limit} rows
                __offset ( db, tablename, offset=200, limit=5 )                                 #OFFSET + LIMIT | note: print only {limit} rows starting from {offset}
                __fetch ( db, tablename, offset=300, fetch=10 )                                 #OFFSET + FETCH | note: print only {limit} rows starting from {offset}
                __where_in ( db, tablename, field='country' )                                   #WHERE + IN | note: employees from a particular country
                __where_between ( db, tablename, field='date_of_birth' )                        #WHERE + BETWEEN | note: employees born on a certain date
                __where_like ( db, tablename, field='email' )                                   #WHERE + LIKE | note: employees with specific {field} format
                __count_groupby ( db, tablename, field='country' )                              #COUNT + GROUP BY | note: amount of employees from counties
                __count_groupby_having ( db, tablename, field='country', amount=8 )             #COUNT + GROUP BY + HAVING + ORDER BY | note: amount of employees(>{amount}) from counties
                __as ( db, tablename, fields=['first_name', 'last_name', 'gender'] )            #AS | note: change headers
                __coalesce ( db, tablename, field='email', msg='not applicable' )               #COALESCE + ORDER BY | note: replace null by {msg}
                __age_now_as ( db, tablename, field='date_of_birth' )                           #AGE + NOW + AS | note: age of employees
                __update_set ( db, tablename, id=1 )                                            #UPDATE + SET + WHERE | note: update {id} row
                tablename = 'holiday'
                __drop_table ( db, tablename )                                                  #DROP TABLE IF EXISTS
                __create_table ( db, tablename )                                                #CREATE TABLE FROM data (SQL)
                __print_table ( db, tablename )                                                 #print table
                __min_max ( db, tablename, field='price', operation='max' )                     #MIN, MAX | note: MIN/MAX price of tickets
                __round_avg ( db, tablename, field='price' )                                    #ROUND + AVERAGE | note: round {field}
                __min_groupby ( db, tablename, fields=['destination_country', 'price'] )        #MIN + GROUP BY | note: most cheap tickets in countries
                __sum_groupby ( db, tablename, fields=['destination_country', 'price'] )        #SUM + GROUP BY | note: amount of tickets to countries
                #TODO: add cars.sql to data. constraint: car make

        except ( Exception, psycopg2.DatabaseError ) as error:
                exit ( error )
