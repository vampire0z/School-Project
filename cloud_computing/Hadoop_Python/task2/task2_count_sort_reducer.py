#!/usr/bin/python3

import sys

def sort_reducer():
	""" This reducer sort top 50 locality in descending order

	Input data format: locality_name1 	\t count_sum
					   locality_name2 	\t count_sum

	Output data format:locality_name_top_1 	\t count_sum
						...
					   locality_name_top_50 \t count_sum

	"""
	# create an empty list to store data
	top_50_list = []

	for line in sys.stdin:

		items = line.strip().split('\t')

		# check the data is in right format
		if len(items) != 2:
			continue

		locality_name, count_sum = items[0].strip(), items[1].strip()

		# appending data to the list as a tuple
		top_50_list.append((locality_name, int(count_sum)))

	# sorted top 50 in descending order
	reversed_list = sorted(top_50_list, key=lambda x : x[1])[-50:]

	# print out
	for name, count in reversed(reversed_list):
		print(name + '\t' + str(count))



if __name__ == '__main__':
	sort_reducer()