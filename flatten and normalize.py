
import json
import requests
from pprint import pprint
import zipfile
import io
import time

# just see the raw response from the url

url = 'https://s3.amazonaws.com/data-eng-homework/v1/data.zip'

r = requests.get(url, stream=True, timeout=10) # add timeout at a later check in


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
order_details_header = ['id', 'order_id', 'product_id', 'quantity', 'variant_id'] # summary.id = details.order_id

# loadable filenames to db 
user_details_file  = 'users.txt'
order_summary_file = 'order_summary.txt'
order_details_file = 'order_details.txt'

for filename in z.namelist():
	#print("reading ", filename)


	if filename == '2017-12-18.json':
	#if 1 == 1:
		data = z.read(filename)
		datadict = json.loads(data)

		# Analysis is pretty much done
		# now i am going to flatten the json and write two files
		# one file for top keys, order summary, another file with order details on summary.id = details.order_id PK

		# my approach is to use a hash for each row of table data and then sort the keys of the dict when writing to the file.


		# for each, order, flatten
		for order in datadict['orders']:

			# i have a new hash for each order
			user_data = {}
			summary_data = {}
			details_data = {}

			#flatten(order)
			for key, val in order.items():

				# def flatten()
				if isinstance(val, list) and key == 'line_items':
					print("need to flatten this one recursive and add current key = id")

				if key in users_details_header:
					user_data[key] = val

				elif key in order_summary_header:
					summary_data[key] = val

				elif key in order_details_header:
					order_data[key] = val

				else:
					print("not using ", key, val)

			# def map and write to file ()
			#map_and_write_to_file(user_data, summary_data, order_data)
			#pprint(user_data)

			# NEXT STEP IS MAPPING AND WRITING TO FILE
			#some_list = [dct[k] for k in lst]
			user_data_list = [user_data[k] for k in users_details_header]
			print("final: ", user_data_list)








