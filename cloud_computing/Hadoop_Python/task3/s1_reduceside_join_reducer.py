#!/usr/bin/python3
# coding: utf-8

import sys

def url_tag_join():
	""" This reducer run reduce side join
	Input format: 	
					place_url_1#0 \t count
					place_url_2#0 \t count
					place_url_2#1 \t tag1 
					place_url_3#0 \t count
					place_url_3#1 \t tag1 
					place_url_3#1 \t tag2 
					place_url_4#0 \t count
					place_url_4#1 \t tag1
					place_url_5#1 \t tag1
					
	Ourput format: 	place_url_2 \t tag
					place_url_3 \t tag
					place_url_4 \t tag
	"""

#	data = read_map_output(sys.stdin)
	current_place_url = ''

	# method 1
	for line in sys.stdin:
		record = line.strip().split('\t')
		#check if the record is valid
		if len(record)!=2:
			continue
		key, value = record[0].strip(), record[1].strip()
		if key == '':
			continue
		key = key.split('#')

		# place_url_1 tag
		# place_url_2 
		# place_url_2 tag
		# place_url_3
		# place_url_3 tag 
		# place_url_4 tag	
		if key[0] != current_place_url:
			if key[1] == '0':
				current_place_url = key[0]
			else: # do nothing with key[1] = 1
				continue
		else:
			# place_url_1#0
			# place_url_1#1 tag 
			print(current_place_url+'\t'+value)
			

		"""
	# Method 2
	for record in data:
		if len(record) == 1:
			print('hi, this length 1')
			current_place_url = record
		elif len(record) == 2:
			print('hi, this length 2')
			key,value = record[0],record[1]
			if key != current_place_url:
				print('hi, key different')
				continue
			else:
				print(current_place_url+'\t'+value)"""


if __name__ == '__main__':
	url_tag_join()





