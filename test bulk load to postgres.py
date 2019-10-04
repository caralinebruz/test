#!/usr/bin/python3

import json
import requests
from pprint import pprint
import zipfile
import io
import time
import logging
import psycopg2

filename = ""
#filename = users_file + 'users_20180205_004937'

c = {
	'host' : '',
	'port'  : 80,
	'user' : 'c',
	'password' : '',
	'database' : 'cdb'
	}

def connect():

	conn = None
	try:
		print("Connecting to PostgreSQL db...")

		conn = psycopg2.connect(
								host	= c['host'],
								database= c['database'], 
								user 	= c['user'], 
								password= c['password']
								)

		# create cursor
		cur = conn.cursor()

		sql = 'ALTER USER c WITH SUPERUSER;'
		#sql = 'COPY users FROM ' + filename + ' WITH (FORMAT csv);'
		print("final sql: ", sql)
		cur.execute(sql)
		#cur.execute('SELECT * from users')
		#rows = cur.fetchall()
		#for row in rows:
		#	print(row)


		#close 
		cur.close()

		# if inserting, commit result
		conn.commit()

	except (Exception, psycopg2.DatabaseError) as error:
		print(error)
	finally:
		if conn is not None:
			conn.close()
			print('Retry close connection')


### MAIN

#z = make_request()
#parse(z)

#dbh = connect()
#load_file()

if __name__ == '__main__':
	connect()



## getting this error:
# Connecting to PostgreSQL db...
# final sql:  COPY users FROM '' WITH (FORMAT csv);
# must be superuser to COPY to or from a file
#HINT:  Anyone can COPY to stdout or from stdin. psql's \copy command also works for anyone.
###
