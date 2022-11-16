
#Execution of various queries to the database (PostgreSQL)

#system libraries
import psycopg2
from pprint import pprint


#drop table
def __drop_table ( db, tablename ) -> None:

        query = f"DROP TABLE IF EXISTS {tablename};"
        db.query ( query )


#create table from data (SQL)
def __create_table ( db, tablename ) -> None:

        query = open ( f'./data/{tablename}.sql', 'r' ).read()
        db.query ( query )


#create table from data (CSV->SQL)
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
def __where_and ( db, tablename, columns ) -> None:

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
def __where_in ( db, tablename, field ) -> None:

        query = f"SELECT * FROM {tablename} WHERE {field} IN ('Canada', 'Peru', 'Israel');"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#where + between
def __where_between ( db, tablename, field ) -> None:

        query = f"SELECT * FROM {tablename} WHERE {field} BETWEEN '1990-01-01' AND '1992-01-01';"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#where + like
def __where_like ( db, tablename, field ) -> None:

        query = f"SELECT * FROM {tablename} WHERE {field} LIKE '%@google.%';"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#count + group by
def __count_groupby ( db, tablename, field ) -> None:

        query = f"SELECT {field}, COUNT(*) FROM {tablename} GROUP BY {field};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#count + group by + having + order by
def __count_groupby_having ( db, tablename, field ) -> None:

        query = f"SELECT {field}, COUNT(*) FROM {tablename} GROUP BY {field} HAVING COUNT(*) > 8 ORDER BY COUNT(*);"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )


#as
def __as ( db, tablename, fields ) -> None:

        query = f"SELECT id, {fields [ 0 ]} AS name, {fields [ 1 ]} AS surname, country, {fields [ 2 ]} AS sex FROM employee;"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 )  


#coalesce + order by
def __coalesce ( db, tablename, field ) -> None:

        query = f"SELECT COALESCE({field}, 'not applicable') FROM employee ORDER BY email;"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 ) 


#min, max
def __min_max ( db, tablename, field, operation ) -> None:

        query = f"SELECT {operation}({field}) FROM {tablename};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 ) 


#round + average
def __round_avg ( db, tablename, field ) -> None:

        query = f"SELECT ROUND(AVG({field})) FROM {tablename};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 ) 


#min + group by | note: most cheap tickets in countries
def __min_groupby ( db, tablename, fields ) -> None:

        query = f"SELECT {fields [ 0 ]}, MIN({fields [ 1 ]}) FROM {tablename} GROUP BY {fields [ 0 ]};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 ) 


#sum + group by | note: amount of tickets to countries
def __sum_groupby ( db, tablename, fields ) -> None:

        query = f"SELECT {fields [ 0 ]}, SUM({fields [ 1 ]}) FROM {tablename} GROUP BY {fields [ 0 ]};"
        print ( '\n', query )
        pprint ( db.query ( query, selection=True ), width=200 ) 


#various queries
def queries ( db ):

        tablename = 'employee'

        try:                                                                                    #TODO: add notes
                __drop_table ( db, tablename )                                                  #drop table if exists
                __create_table ( db, tablename )                                                #create table from data (SQL)
                __drop_table ( db, tablename )                                                  #drop table if exists
                __create_table_csv ( db, tablename )                                            #create table from data (CSV->SQL)
                __print_table ( db, tablename )                                                 #print table
                __print_columns ( db, tablename )                                               #print table column names
                __column ( db, tablename, column='email' )                                      #print one column
                __columns ( db, tablename, columns=['first_name', 'last_name', 'title'] )       #print several columns
                __orderby ( db, tablename, orderby='city', sort='DESC' )                        #print table order by {field} (ASC/DESC)
                __distinct ( db, tablename, column='country', sort='ASC' )                      #distinct rows in column
                __where_and ( db, tablename, columns=['gender', 'country'] )                    #where + and
                __conditions ( db, tablename, columns=['gender', 'country', 'first_name'] )     #where + conditions + order by
                __limit ( db, tablename, limit=12 )                                             #limit
                __offset ( db, tablename, offset=200, limit=5 )                                 #offset + limit
                __fetch ( db, tablename, offset=300, fetch=10 )                                 #offset + fetch
                __where_in ( db, tablename, field='country' )                                   #where + in
                __where_between ( db, tablename, field='date_of_birth' )                        #where + between
                __where_like ( db, tablename, field='email' )                                   #where + like
                __count_groupby ( db, tablename, field='country' )                              #count + group by
                __count_groupby_having ( db, tablename, field='country' )                       #count + group by + having + order by
                __as ( db, tablename, fields=['first_name', 'last_name', 'gender'] )            #as
                __coalesce ( db, tablename, field='email' )                                     #coalesce + order by
                tablename = 'holiday'
                __drop_table ( db, tablename )                                                  #drop table if exists
                __create_table ( db, tablename )                                                #create table from data (SQL)
                __print_table ( db, tablename )                                                 #create table from data (SQL)
                __min_max ( db, tablename, field='price', operation='max' )                     #min, max
                __round_avg ( db, tablename, field='price' )                                    #round + average
                __min_groupby ( db, tablename, fields=['destination_country', 'price'] )        #min + group by | note: most cheap tickets in countries
                __sum_groupby ( db, tablename, fields=['destination_country', 'price'] )        #sum + group by | note: amount of tickets to countries

        except ( Exception, psycopg2.DatabaseError ) as error:
                exit ( error )
