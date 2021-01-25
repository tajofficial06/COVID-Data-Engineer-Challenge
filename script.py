#!/usr/bin/env python3

import requests
import sqlite3
import time
import multiprocessing

start = time.time()


API_ENDPOINT = "https://health.data.ny.gov/api/views/xdss-u53e/rows.json"

# response from API in json format

API_RESPONSE = requests.get(API_ENDPOINT).json()


# Let's declare a list where we will store all county 

county = []

for i in range(len(API_RESPONSE['data'])):
     county.append(API_RESPONSE['data'][i][9])

#remove duplicate value from list
county=list(set(county))

#sort the list in alphabetical order
county.sort()




# Function to create a database connection 

def create_connection():
    """ create a database connection to the SQLite database
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect("sqlite.db")
        print("Database connection successful...")
        return conn
    except Error as e:
        print("Error in connecting to SQLite database")
        print(e)
    return conn

# function to create table. This fuction will be called for each county in the array list so that all county tables will be created. 

def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


# create a database connection
conn = create_connection()


# Function to  insert data into table.  

def insert_rows(county):
        count = 0
        for i in range(len(API_RESPONSE['data'])):
            if API_RESPONSE['data'][i][9] == county:
                test_date = API_RESPONSE['data'][i][8]
                positive_cases = API_RESPONSE['data'][i][10]
                cumm_pos_cases = API_RESPONSE['data'][i][11]
                total_test = API_RESPONSE['data'][i][12]
                cumm_total_test = API_RESPONSE['data'][i][13]

                insert_statement_sql = """ INSERT OR REPLACE INTO `{}` (test_date,positive_cases,cumm_pos_cases,total_test,cumm_total_test) VALUES ('{}',{},{},{},{});""".format(county,test_date,positive_cases,cumm_pos_cases,total_test,cumm_total_test)

                # insert data to table 
                try:
                    c = conn.cursor()
                    c.execute(insert_statement_sql)
                except Error as e:
                    print(e)

                #insert_data(conn,insert_statement_sql)
                count += 1
        #commit once all rows are inserted in a table
        print("Inserted/Updated {} rows in table {} ".format(count,county))
        conn.commit()
        


    

##
# Call insert statement via parallel processing. Here I have used processes = 6 . This value should not excced your cpu cores count.  At this time this API doesn't take much time to respond 
# so increasing parallel processing is not that affective. 
#


if __name__ == '__main__':

    pool = multiprocessing.Pool()
    pool = multiprocessing.Pool(processes=6)

    if conn is not None:
        for name in county:
            create_table_sql = """ CREATE TABLE IF NOT EXISTS  `{}` (
                                test_date TEXT PRIMARY KEY,
                                positive_cases integer,
                                cumm_pos_cases integer,
                                total_test integer,
                                cumm_total_test integer,
                                load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
                                );""".format(name)
            create_table(conn,create_table_sql)
            print("Table "+ name + " Created.")
    else:
        print("Error in creating tables")


    # comment out below 2 lines for serial execution 
    
    # for county in county:
    #   insert_rows(county)

    pool.map(insert_rows, county)

    print("***************************************")
    print('Total Execution time : {0:0.1f} seconds'.format(time.time() - start))
    print("***************************************")

    pool.close()
    conn.close()