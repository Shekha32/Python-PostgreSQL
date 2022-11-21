
#Execution of various queries to the database (PostgreSQL)

#system libraries
import psycopg2
from pprint import pprint


#DROP TABLE IF EXISTS
def __drop_table ( db, table ) -> None:

        query = f"DROP TABLE IF EXISTS {table};"
        db.query ( query )


#CREATE TABLE FROM data (SQL)
def __create_table ( db, table ) -> None:

        query = open ( f'./data/{table}.sql', 'r' ).read()
        db.query ( query )


#CREATE TABLE FROM data (CSV->SQL)
def __create_table_csv ( db, table ) -> None:

        head = f"CREATE TABLE {table} ( " + \
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
                        lines += f"insert into {table} ({fields}) values ({line});"

        query = head + lines
        db.query ( query )


#DELETE row
def __delete_row ( db, table, id ) -> None:

        query = f"DELETE FROM {table} WHERE id={id};"
        print ( '\n', query )
        db.query ( query )


#UPDATE + SET + WHERE | note: update {id} row
def __update_set ( db, table, id ) -> None:

        query = f"UPDATE {table} SET first_name = 'Alex', last_name = 'Shekha', email = 'le.shekha@mail.com' WHERE id = {id};"
        print ( '\n', query )
        pprint ( db.query ( query ), width=200 ) 


#ALTER TABLE - ADD CONSTRAINT + CHECK | note: gender constraint
def __add_constaint_check ( db, table, field ) -> None:

        query = f"ALTER TABLE {table} ADD CONSTRAINT {field}_constraint CHECK (gender = 'Male' OR gender = 'Female');"
        print ( '\n', query )
        pprint ( db.query ( query ), width=200 )


#INSERT row + CONFLICT handling
def __conflict ( db, table ) -> None:

        query = f"INSERT INTO {table} (id, first_name, last_name, date_of_birth, gender, city, country, title) " + \
                "VALUES (2, 'Le', 'Shekha', '2003-03-04', 'Male', 'Oslo', 'Croatia', 'Backend Developer') " + \
                "ON CONFLICT (id) DO UPDATE SET first_name = EXCLUDED.first_name, gender = EXCLUDED.gender;"
                #"ON CONFLICT (id) DO NOTHING;"
                
        print ( '\n', query )
        pprint ( db.query ( query ), width=200 ) 


#ALTER TABLE - remove PRIMARY KEY
def __remove_primary_key ( db, table ) -> None:

        query = f"ALTER TABLE {table} DROP CONSTRAINT {table}_pkey;"
        print ( '\n', query )
        db.query ( query )


#ADD PRIMARY KEY
def __add_primary_key ( db, table, field ) -> None:

        query = f"ALTER TABLE {table} ADD PRIMARY KEY({field});"
        print ( '\n', query )
        db.query ( query )


#ALTER TABLE - ADD CONSTRAINT unique field
def __unique_field ( db, table, field ) -> None:

        query = f"ALTER TABLE {table} ADD CONSTRAINT unique_{field} UNIQUE({field});"
        print ( '\n', query )
        db.query ( query )


#ALTER TABLE - ADD unique field
def __unique_key ( db, table, field ) -> None:

        query = f"ALTER TABLE {table} ADD UNIQUE({field});"
        print ( '\n', query )
        db.query ( query )


#print table
def __print_table ( db, table ) -> None:

        query = f"SELECT * FROM {table};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#print table column names
def __print_columns ( db, table ) -> None:

        query = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}';"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#print one column
def __column ( db, table, column ) -> None:

        query = f"SELECT {column} FROM {table};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#print several columns
def __columns ( db, table, columns ) -> None:

        query = f"SELECT {','.join ( columns )} FROM {table};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#print table ORDER BY {field} (ASC/DESC)
def __orderby ( db, table, orderby, sort ) -> None:

        query = f"SELECT * FROM {table} ORDER BY {orderby} {sort};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#DISTINCT rows in column
def __distinct ( db, table, column, sort ) -> None:

        query = f"SELECT DISTINCT {column} FROM {table} ORDER BY {column} {sort};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#WHERE + AND
def __where_and ( db, table, columns ) -> None:

        query = f"SELECT * FROM {table} WHERE {columns [ 0 ]}='Female' AND {columns [ 1 ]}='Brazil';"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#WHERE + conditions + ORDER BY
def __conditions ( db, table, columns ) -> None:

        query = f"SELECT * FROM {table} WHERE {columns [ 0 ]}='Female' AND ({columns [ 1 ]}='Poland' OR {columns [ 1 ]}='Germany') ORDER BY {columns [ 2 ]};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#LIMIT | note: print only {limit} rows
def __limit ( db, table, limit ) -> None:

        query = f"SELECT * FROM {table} LIMIT {limit};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#OFFSET + LIMIT | note: print only {limit} rows starting from {offset}
def __offset ( db, table, offset, limit ) -> None:

        query = f"SELECT * FROM {table} OFFSET {offset} LIMIT {limit};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#OFFSET + FETCH | note: print only {limit} rows starting from {offset}
def __fetch ( db, table, offset, fetch ) -> None:

        query = f"SELECT * FROM {table} OFFSET {offset} FETCH FIRST {fetch} ROW ONLY;"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#WHERE + IN | note: employees from a particular country
def __where_in ( db, table, field ) -> None:

        query = f"SELECT * FROM {table} WHERE {field} IN ('Canada', 'Peru', 'Israel');"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#WHERE + BETWEEN | note: employees born on a certain date
def __where_between ( db, table, field ) -> None:

        query = f"SELECT * FROM {table} WHERE {field} BETWEEN '1990-01-01' AND '1992-01-01';"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#WHERE + LIKE | note: employees with specific {field} format
def __where_like ( db, table, field ) -> None:

        query = f"SELECT * FROM {table} WHERE {field} LIKE '%@google.%';"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#COUNT + GROUP BY | note: amount of employees from counties
def __count_groupby ( db, table, field ) -> None:

        query = f"SELECT {field}, COUNT(*) FROM {table} GROUP BY {field};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#COUNT + GROUP BY + HAVING + ORDER BY | note: amount of employees(>{amount}) from counties
def __count_groupby_having ( db, table, field, amount ) -> None:

        query = f"SELECT {field}, COUNT(*) FROM {table} GROUP BY {field} HAVING COUNT(*) > {amount} ORDER BY COUNT(*);"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#AS | note: change headers
def __as ( db, table, fields ) -> None:

        query = f"SELECT id, {fields [ 0 ]} AS name, {fields [ 1 ]} AS surname, country, {fields [ 2 ]} AS sex FROM employee;"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )  


#COALESCE + ORDER BY | note: replace null by {msg}
def __coalesce ( db, table, field, msg ) -> None:

        query = f"SELECT COALESCE({field}, '{msg}') FROM employee ORDER BY email;"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 ) 


#AGE + NOW + AS | note: age of employees
def __age_now_as ( db, table, field ) -> None:

        query = f"SELECT first_name, last_name, title, AGE(NOW(), {field}) AS age FROM {table};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 ) 


#MIN, MAX | note: MIN/MAX price of tickets
def __min_max ( db, table, field, operation ) -> None:

        query = f"SELECT {operation}({field}) FROM {table};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 ) 


#ROUND + AVERAGE | note: round {field}
def __round_avg ( db, table, field ) -> None:

        query = f"SELECT ROUND(AVG({field})) FROM {table};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 ) 


#MIN + GROUP BY | note: most cheap tickets in countries
def __min_groupby ( db, table, fields ) -> None:

        query = f"SELECT {fields [ 0 ]}, MIN({fields [ 1 ]}) FROM {table} GROUP BY {fields [ 0 ]};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 ) 


#SUM + GROUP BY | note: amount of tickets to countries
def __sum_groupby ( db, table, fields ) -> None:

        query = f"SELECT {fields [ 0 ]}, SUM({fields [ 1 ]}) FROM {table} GROUP BY {fields [ 0 ]};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 ) 


#ALTER TABLE - ADD REFERENCES | note: add foreign key from car(id) to employee
def __add_references ( db, tables, field ) -> None:

        query = f"ALTER TABLE {tables [ 0 ]} ADD {tables [ 1 ]}_id INT REFERENCES {tables [ 1 ]} ({field});"
        print ( '\n', query )
        pprint ( db.query ( query ), width=200 ) 


#UPDATE + SET | note: add car_id to employee
def __update_set_car ( db, table, field, id ) -> None:

        query = f"UPDATE {table} SET {field} = {id [ 0 ]} WHERE id = {id [ 1 ]} AND {table}.email IS NOT NULL;"
        print ( '\n', query )
        pprint ( db.query ( query ), width=200 )


#INNER JOIN | note: print employees with cars
def __inner_join ( db, tables, field ) -> None:

        query = f"SELECT * FROM {tables [ 0 ]} JOIN {tables [ 1 ]} ON {tables [ 0 ]}.{field} = car.id;"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#INNER JOIN | note: print different fields
def __inner_join2 ( db, tables, fields ) -> None:

        query = f"SELECT {tables [ 0 ]}.{fields [ 0 ]}, {tables [ 1 ]}.{fields [ 1 ]}, {tables [ 1 ]}.{fields [ 2 ]} " + \
                f"FROM {tables [ 0 ]} JOIN {tables [ 1 ]} ON {tables [ 0 ]}.car_id = car.id;"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#various queries
def queries ( db ):

        table = 'employee'

        try:
                #creation and settings
                __drop_table ( db, table )                                                      #DROP TABLE IF EXISTS
                __create_table ( db, table )                                                    #CREATE TABLE FROM data (SQL)
                __drop_table ( db, table )                                                      #DROP TABLE IF EXISTS
                __create_table_csv ( db, table )                                                #CREATE TABLE FROM data (CSV->SQL)
                __delete_row ( db, table, id='500' )                                            #DELETE row
                __update_set ( db, table, id=1 )                                                #UPDATE + SET + WHERE | note: update {id} row
                __add_constaint_check ( db, table, field='gender' )                             #ALTER TABLE - ADD CONSTRAINT + CHECK | note: gender constraint
                __conflict ( db, table )                                                        #INSERT row + CONFLICT handling
                __remove_primary_key ( db, table )                                              #ALTER TABLE - remove PRIMARY KEY
                __add_primary_key ( db, table, field='id' )                                     #ADD PRIMARY KEY
                __unique_field ( db, table, field='email' )                                     #ALTER TABLE - ADD CONSTRAINT unique field | note: email is unique field
                #selection
                __print_table ( db, table )                                                     #print table
                __print_columns ( db, table )                                                   #print table column names
                __column ( db, table, column='email' )                                          #print one column
                __columns ( db, table, columns=['first_name', 'last_name', 'title'] )           #print several columns
                __orderby ( db, table, orderby='city', sort='DESC' )                            #print table ORDER BY {field} (ASC/DESC)
                __distinct ( db, table, column='country', sort='ASC' )                          #DISTINCT rows in column
                __where_and ( db, table, columns=['gender', 'country'] )                        #WHERE + AND
                __conditions ( db, table, columns=['gender', 'country', 'first_name'] )         #WHERE + conditions + ORDER BY
                __limit ( db, table, limit=12 )                                                 #LIMIT | note: print only {limit} rows
                __offset ( db, table, offset=200, limit=5 )                                     #OFFSET + LIMIT | note: print only {limit} rows starting from {offset}
                __fetch ( db, table, offset=300, fetch=10 )                                     #OFFSET + FETCH | note: print only {limit} rows starting from {offset}
                __where_in ( db, table, field='country' )                                       #WHERE + IN | note: employees from a particular country
                __where_between ( db, table, field='date_of_birth' )                            #WHERE + BETWEEN | note: employees born on a certain date
                __where_like ( db, table, field='email' )                                       #WHERE + LIKE | note: employees with specific {field} format
                __count_groupby ( db, table, field='country' )                                  #COUNT + GROUP BY | note: amount of employees from counties
                __count_groupby_having ( db, table, field='country', amount=8 )                 #COUNT + GROUP BY + HAVING + ORDER BY | note: amount of employees(>{amount}) from counties
                __as ( db, table, fields=['first_name', 'last_name', 'gender'] )                #AS | note: change headers
                __coalesce ( db, table, field='email', msg='not applicable' )                   #COALESCE + ORDER BY | note: replace null by {msg}
                __age_now_as ( db, table, field='date_of_birth' )                               #AGE + NOW + AS | note: age of employees
                #creation and settings
                table = 'holiday'
                __drop_table ( db, table )                                                      #DROP TABLE IF EXISTS
                __create_table ( db, table )                                                    #CREATE TABLE FROM data (SQL)
                #selection
                __print_table ( db, table )                                                     #print table
                __min_max ( db, table, field='price', operation='max' )                         #MIN, MAX | note: MIN/MAX price of tickets
                __round_avg ( db, table, field='price' )                                        #ROUND + AVERAGE | note: round {field}
                __min_groupby ( db, table, fields=['destination_country', 'price'] )            #MIN + GROUP BY | note: most cheap tickets in countries
                __sum_groupby ( db, table, fields=['destination_country', 'price'] )            #SUM + GROUP BY | note: amount of tickets to countries
                #creation and settings
                table, tables = 'car', [ 'employee', 'car' ]
                __drop_table ( db, table )                                                      #DROP TABLE IF EXISTS
                __create_table ( db, table )                                                    #CREATE TABLE FROM data (SQL)
                __add_references ( db, tables, field='id' )                                     #ALTER TABLE - ADD REFERENCES | note: add foreign key from car(id) to employee
                __unique_key ( db, table='employee', field='car_id' )                           #ALTER TABLE - ADD unique field | note: foreign key from car(id) is unique field
                #selection
                __print_table ( db, table )                                                     #print table
                __update_set_car ( db, table='employee', field='car_id', id=[93, 490] )         #UPDATE + SET | note: add car_id to employee
                __inner_join ( db, tables, field='car_id' )                                     #INNER JOIN | note: print employees with cars
                __inner_join2 ( db, tables, fields=['last_name', 'make', 'model'] )             #INNER JOIN | note: print different fields

        except ( Exception, psycopg2.DatabaseError ) as error:
                exit ( error )
