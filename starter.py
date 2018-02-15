
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

for filename in z.namelist():
	#print(filename)

	if filename == '2017-12-18.json':
		data = z.read(filename)
		datadict = json.loads(data)
		#pprint(datadict)

		# ok so at this point, analysis begins on the data.
		# how should i store this? transactional star schema? user data, transaction, order details

		# FOR DOING ALL OF THEM
		#for order in datadict['orders']:
		#	print(order.keys())

		# FOR SEEING ONE OF THEM
		orders_list = datadict['orders'][0]
		print(orders_list.keys())
		pprint(orders_list)

		for key in orders_list.keys()




