import requests
import sqlite3
import json


# check if API endpoint response is 200 

def test_api_response_200():
     res = requests.get("https://health.data.ny.gov/api/views/xdss-u53e/rows.json")
     assert res.status_code == 200

# test for database connection 

def test_db_connection():
    conn = None
    try:
        conn = sqlite3.connect("sqlite.db")
        print("Database connection successful...")
        return conn
    except Error as e:
        print("Error in connecting to SQLite database")
        print(e)
    return conn


# test for table creation 

def test_create_table():
     conn = sqlite3.connect("sqlite.db")
     create_table_sql = """CREATE TABLE IF NOT EXISTS  test (
     test_date TEXT PRIMARY KEY,
     positive_cases integer,
     cumm_pos_cases integer,
     total_test integer,
     cumm_total_test integer,
     load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
     );"""
     try:
        c = conn.cursor()
        c.execute(create_table_sql)
     except Error as e:
        print(e)



# mocking rsponse received from API Endpoint so that if order of columns changes then insert will fail 
res_obj = '''
{
	"meta": {
		"view": "data"
	},
	"data": [
		[
			"row-6rtz.cusn-236x",
			"00000000-0000-0000-D4E5-A5871E9BB6F5",
			0,
			1611426649,
			null,
			1611426649,
			null,
			"{ }",
			"2020-03-01T00:00:00",
			"Albany",
			"0",
			"1",
			"3",
			"4"
		],
		[
			"row-3by9~22yt_w8ve",
			"00000000-0000-0000-EEE2-39B483DDE999",
			0,
			1611426649,
			null,
			1611426649,
			null,
			"{ }",
			"2020-03-02T00:00:00",
			"Albany",
			"3",
			"2",
			"4",
			"5"
		]
	]
}
'''

res_obj=json.loads(res_obj)

# Insert test for mock response data 

def test_insert_data():
     conn = sqlite3.connect("sqlite.db")
     
     for i in range(len(res_obj['data'])):
          if res_obj['data'][i][9] == "Albany":
               test_date = res_obj['data'][i][8]
               positive_cases = res_obj['data'][i][10]
               cumm_pos_cases = res_obj['data'][i][11]
               total_test = res_obj['data'][i][12]
               cumm_total_test = res_obj['data'][i][13]

               insert_statement_sql = """ INSERT OR REPLACE INTO test (test_date,positive_cases,cumm_pos_cases,total_test,cumm_total_test) VALUES ('{}',{},{},{},{});""".format(test_date,positive_cases,cumm_pos_cases,total_test,cumm_total_test)
     try:
        c = conn.cursor()
        c.execute(insert_statement_sql)
     except Error as e:
        print(e)

def test_delete_table():
     conn = sqlite3.connect("sqlite.db")
     try:
        c = conn.cursor()
        c.execute("drop table test")
     except Error as e:
        print(e)