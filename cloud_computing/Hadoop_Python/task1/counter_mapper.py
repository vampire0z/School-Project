#!/usr/bin/python3

import sys

def count_mapper():
	""" This mapper select location url from the reduceside_join_reducer output and add a counter on it
	Input data format: place_url \t place_type_id \t tags  
	(with place_type_id only in '7' and '22')
	Output data format: localityName \t 1  
	(where localityName is the reformat of place_url)
	"""

	for line in sys.stdin:
		
		items = line.strip().split('\t')

		# check input data is in right format
		if len(items) != 3:
			continue

		place_url, place_type_id = items[0].strip(), items[1].strip()

		# check input data is valid for our output
		if place_url == 'Place_not_found':
			continue

		split_place_url = place_url.strip().split('/')

		localityName = ''
		# check place id of format, distinct  neighbourhood('22') and  locality('7')
		if place_type_id == '7':
			
			# reformat the local url into locality name
			for item in split_place_url[2:]:
				localityName = item + ', ' + localityName
			localityName = localityName + split_place_url[1]

			print(localityName.replace('+',' ') + '\t' + '1')

		else:
			# reformat the local url into locality name without the neighbourhood address
			for item in split_place_url[2:-1]:
				localityName = item + ', ' + localityName
			localityName = localityName + split_place_url[1]
			
			print(localityName.replace('+',' ') + '\t' + '1')

if __name__ == '__main__':
	count_mapper()