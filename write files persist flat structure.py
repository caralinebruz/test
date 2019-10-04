#!/usr/bin/python3

import json
import requests
from pprint import pprint
import zipfile
import io
import time
import logging

# just see the raw response from the url

url = ''

r = requests.get(url, stream=True, timeout=10) # add timeout at a later check in


dts = time.strftime('_%Y%m%d_%H%M%S')
# avoid partially reading request streams / make sure it is always closed!
#with requests.get('http://httpbin.org/get', stream=True) as r:
    # Do things with the response here.

pprint(r.headers)

if r.ok:
	print("yes")
else:
	r.raise_for_status()

print(r.ok)

#with zipfile.ZipFile(io.BytesIO(r.content)) as myzip:
#	with myzip.open(this) as myfile:
#		print(myfile.read())

z = zipfile.ZipFile(io.BytesIO(r.content))

#print(z.namelist()) # dates

# in the discussion, say that i have to assume that all orders are structured/stored simialr way in s3, 
users_details_header = ['user_id', 'email', 'buyer_accepts_marketing', 'name', 'phone', 'customer_locale', 'contact_email'] # users.id = summary.user_id
order_summary_header = ['id', 'closed_at', 'created_at', 'updated_at', 'number', 'note', 'token', 'gateway', 'test', 'total_price', 'subtotal_price', 'total_weight', 'total_tax', 'taxes_included', 'currency', 'financial_status', 'confirmed', 'total_discounts', 'total_line_items_price', 'cart_token', 'referring_site', 'landing_site', 'cancelled_at', 'cancel_reason', 'total_price_usd', 'checkout_token', 'reference', 'user_id', 'location_id', 'source_identifier', 'source_url', 'processed_at', 'device_id', 'app_id', 'browser_ip', 'landing_site_ref', 'order_number', 'processing_method', 'checkout_id', 'source_name', 'fulfillment_status', 'tags', 'order_status_url', 'total_discount']
order_details_header = ['id', 'summary_id', 'product_id', 'quantity', 'variant_id'] # summary.id = details.summary_id

# loadable filenames to db 
path = ''
user_details_file  = path + 'users' + dts + '.txt'
order_summary_file = path + 'order_summary' + dts + '.txt'
order_details_file = path + 'order_details' + dts + '.txt'

# set for seen user ids
seen_users = set()

def write_flat_file(row, filename):

	with open(filename, 'at') as f:
		#print(row, file=f)
		print(*row, sep=',', file=f)
		#f.write(string)



def flatten_detail(order_id, item):

	details_data = {}
	# import the order id for this item
	details_data['summary_id'] = order_id

	# now go through each item and 
	for key, val in item.items():

		if key in order_details_header:
			details_data[key] = val

	row_od = [details_data[k] for k in order_details_header]
	#pprint(row_od)
	# write row to file (row_od, filename)
	write_flat_file(row_od, order_details_file)


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

			#flatten(order)
			for key, val in order.items():

				# def flatten()
				if isinstance(val, list) and key == 'line_items':
					#print("need to flatten this one recursive and add current key = id")
					for item in val:

						#pprint(item)
						order_id = order['id']
						# flatten single detail, bring in the order id O(1)
						flatten_detail(order_id, item)

				if key in users_details_header:
					user_data[key] = val

				if key in order_summary_header:
					summary_data[key] = val

				# this SHOULD not happen until we flatten it.
				#if key in order_details_header:
				#	details_data[key] = val

				#else:
				#	print("not using ", key, val)

			# def map and write to file ()
			#map_and_write_to_file(user_data, summary_data, order_data)
			#pprint(user_data)

			# NEXT STEP IS MAPPING AND WRITING TO FILE
			#some_list = [dct[k] for k in lst]
			current_user = user_data['user_id']
			if current_user not in seen_users:
				#print("not yet seen")

				# some function to write the row to file
				row_u = [user_data[k] for k in users_details_header]
				write_flat_file(row_u, user_details_file)

				# write row to file (row_u, filename)
				# finally add it to the seen set
				seen_users.add(current_user)

			#else:
			#	print("skipping existing user row: ", current_user)


			# STEPS FOR ORDER SUMMARY
			# most basic case
			row_os = [summary_data[k] for k in order_summary_header]
			#print(row_os)
			# write row to file (row_os, filename)
			write_flat_file(row_os, order_summary_file)


# EOM So, i think i need to do the order details flattening AFTER i have the summary_id (id key from summary row)








