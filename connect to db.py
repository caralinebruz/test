#!/usr/bin/python3

import json
import requests
from pprint import pprint
import zipfile
import io
import time
import logging
import psycopg2



def make_request():
	url = 'https://s3.amazonaws.com/data-eng-homework/v1/data.zip'
	r = requests.get(url, stream=True, timeout=10)

	pprint(r.headers)

	if r.ok:
		print("yes")
	else:
		r.raise_for_status()

	print(r.ok)

	z = zipfile.ZipFile(io.BytesIO(r.content))
	return(z)

# i assume that all orders are structured/nested simialr way in s3, 
users_details_header = ['user_id', 'email', 'buyer_accepts_marketing', 'name', 'phone', 'customer_locale', 'contact_email'] # users.id = summary.user_id
order_summary_header = ['id', 'closed_at', 'created_at', 'updated_at', 'number', 'note', 'token', 'gateway', 'test', 'total_price', 'subtotal_price', 'total_weight', 'total_tax', 'taxes_included', 'currency', 'financial_status', 'confirmed', 'total_discounts', 'total_line_items_price', 'cart_token', 'referring_site', 'landing_site', 'cancelled_at', 'cancel_reason', 'total_price_usd', 'checkout_token', 'reference', 'user_id', 'location_id', 'source_identifier', 'source_url', 'processed_at', 'device_id', 'app_id', 'browser_ip', 'landing_site_ref', 'order_number', 'processing_method', 'checkout_id', 'source_name', 'fulfillment_status', 'tags', 'order_status_url', 'total_discount']
order_details_header = ['id', 'summary_id', 'product_id', 'quantity', 'variant_id'] # summary.id = details.summary_id

# loadable filenames to db 
dts = time.strftime('_%Y%m%d_%H%M%S')
path = 'C:\\Users\\caraline\\Desktop\\glossier\\'
ext = dts + '.txt'
user_details_file  = path + 'users' + ext
order_summary_file = path + 'order_summary' + ext
order_details_file = path + 'order_details' + ext

# set for seen user ids
seen_users = set()

def write_flat_file(row, filename):

	with open(filename, 'at') as f:
		print(*row, sep=',', file=f)


def flatten_detail(order_id, item):

	details_data = {}
	# import the order id for this item
	details_data['summary_id'] = order_id

	# now go through each item and 
	for key, val in item.items():

		if key in order_details_header:
			details_data[key] = val

	row_od = [details_data[k] for k in order_details_header]
	write_flat_file(row_od, order_details_file)


def parse(z):
	for filename in z.namelist():
		#print("reading ", filename)

		if filename == '2017-12-18.json':
		#if 1 == 1:
			data = z.read(filename)
			datadict = json.loads(data)

			# for each, order, flatten
			for order in datadict['orders']:

				# i have a new hash for each order, I DO need to set empty each time.
				user_data = {}
				summary_data = {}
				#details_data = {} need this to be created more locally, smaller scope

				for key, val in order.items():

					if isinstance(val, list) and key == 'line_items':
						for item in val:

							order_id = order['id']
							# flatten single detail, bring in the order id O(1)
							flatten_detail(order_id, item)

					if key in users_details_header:
						user_data[key] = val

					if key in order_summary_header:
						summary_data[key] = val

				current_user = user_data['user_id']
				if current_user not in seen_users:
					#print("not yet seen")

					row_u = [user_data[k] for k in users_details_header]
					write_flat_file(row_u, user_details_file)

					# finally add it to the seen set
					seen_users.add(current_user)

				# STEPS FOR ORDER SUMMARY
				# most basic case
				row_os = [summary_data[k] for k in order_summary_header]
				write_flat_file(row_os, order_summary_file)


c = {
	'host' : 'datacandidatehomework.czwbfb7cwdaf.us-east-1.rds.amazonaws.com',
	'port'  : 5432,
	'user' : 'caraline',
	'password' : 'iheartmilkyjelly',
	'database' : 'caraline_db'
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

		#print("Random query")
		#cur.execute('SELECT 1')

		#res = cur.fetchone()
		#print(res)

		#for command in commands:
		#	cur.execute(command)

		# round 3, check the tables are there.
		print("Check if the tables are there")
		cur.execute('SELECT * FROM pg_catalog.pg_tables')
		rows = cur.fetchall()
		for row in rows:
			print(row)


		#close 
		cur.close()

		# if inserting, commit result
		#conn.commit()

	except (Exception, psycopg2.DatabaseError) as error:
		print(error)
	finally:
		if conn is not None:
			conn.close()
			print('Retry close connection')

# excellent opportunity for me to put this into another module.
#def create_tables():
	# if they dont exist

commands = (
		"""
		CREATE TABLE users (
				user_id INTEGER PRIMARY KEY, 
				email VARCHAR(255), 
				buyer_accepts_marketing VARCHAR(16), 
				name VARCHAR(255), 
				phone VARCHAR(255), 
				customer_locale VARCHAR(1000), 
				contact_email VARCHAR(255)
		)
		""",
		"""
		CREATE TABLE order_summary (
				id INTEGER PRIMARY KEY,
				closed_at TIMESTAMP,
				created_at TIMESTAMP,
				updated_at TIMESTAMP,
				number INTEGER,
				note VARCHAR(1000),
				token VARCHAR(255),
				gateway VARCHAR(255),
				test VARCHAR(8),
				total_price FLOAT,
				subtotal_price FLOAT,
				total_weight FLOAT,
				total_tax FLOAT,
				taxes_included VARCHAR(8),
				currency VARCHAR(4),
				financial_status VARCHAR(16),
				confirmed VARCHAR(8),
				total_discounts INTEGER,
				total_line_items_price INTEGER,
				cart_token VARCHAR(255),
				referring_site VARCHAR(255),
				landing_site VARCHAR(255),
				cancelled_at TIMESTAMP,
				cancel_reason VARCHAR(255),
				total_price_usd FLOAT,
				checkout_token VARCHAR(255),
				reference VARCHAR(255),
				user_id INTEGER,
				location_id INTEGER,
				source_identifier VARCHAR(255), 
				source_url VARCHAR(255),
				processed_at TIMESTAMP,
				device_id INTEGER,
				app_id INTEGER,
				browser_ip VARCHAR(255),
				landing_site_ref VARCHAR(255),
				order_number INTEGER,
				processing_method VARCHAR(255),
				checkout_id INTEGER,
				source_name VARCHAR(8),
				fulfillment_status VARCHAR(16),
				tags VARCHAR(255),
				order_status_url VARCHAR(255),
				total_discount FLOAT
		)
		""",
		"""
		CREATE TABLE order_detail (
				id INTEGER PRIMARY KEY,
				summary_id INTEGER,
				product_id INTEGER,
				quantity INTEGER,
				variant_id INTEGER
		)
		"""
		)


### MAIN

#z = make_request()
#parse(z)

#dbh = connect()
#load_file()

if __name__ == '__main__':
	connect()
