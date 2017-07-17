#!/usr/bin/python3

import sys

def sort_mapper():
	""" This mapper read the input from multiple reducer files
	"""

	# Test input form reducer multiple file
	for line in sys.stdin:
		items = line.strip().split('\t')
		locality_name, count_sum = items[0].strip(), items[1].strip()
		print(locality_name + '\t' + count_sum)



if __name__ == '__main__':
	sort_mapper()