
import json
import requests
from pprint import pprint
import zipfile
import io

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

# for finding dupes
d_user_id = {}
d_order_number = {}
d_number = {}
d_id = {}
d_checkout_id = {}
d_app_id = {}
checkkeys = ['user_id', 'order_number', 'number', 'id', 'checkout_id', 'app_id']

for filename in z.namelist():
	#print(filename)

#	if filename == '2017-12-18.json':
	if 1 == 1:
		data = z.read(filename)
		datadict = json.loads(data)
		#pprint(datadict)

		# ok so at this point, analysis begins on the data.
		# how should i store this? transactional star schema? user data, transaction, order details

		# FOR DOING ALL OF THEM
		for order in datadict['orders']:
			print(order.keys())
			for key, val in order.items():

				# is USER_ID good for use as table key?
				if key == 'user_id':
					if val in d_user_id.keys():
						d_user_id[val] += 1
					else:
						d_user_id[val] = 1

				# is ORDER NUMBER good for use as table key?
				if key == 'order_number':
					if val in d_order_number.keys():
						d_order_number[val] += 1
					else:
						d_order_number[val] = 1

				# is HUMBER good for use as table key?
				if key == 'number':
					if val in d_number.keys():
						d_number[val] += 1
					else:
						d_number[val] = 1

				# is ID good for use as table key?
				if key == 'id':
					if val in d_id.keys():
						d_id[val] += 1
					else:
						d_id[val] = 1

				# is checkout_id good for use as table key?
				if key == 'checkout_id':
					if val in d_checkout_id.keys():
						d_checkout_id[val] += 1
					else:
						d_checkout_id[val] = 1

				# is ID good for use as table key?
				if key == 'app_id':
					if val in d_app_id.keys():
						d_app_id[val] += 1
					else:
						d_app_id[val] = 1


#pprint(d_user_id)
#pprint(d_order_number)
#pprint(d_number)
pprint(d_id)
#pprint(d_checkout_id)
#pprint(d_app_id) #one key, 129785, lol they sent me 25000 orders in all the files!


		# FOR SEEING ONE OF THEM
		#orders_list = datadict['orders'][0]
		#print(orders_list.keys())
		#pprint(orders_list)

		#for key in orders_list.keys()





