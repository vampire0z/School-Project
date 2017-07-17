#!/usr/bin/python3

import sys

def count_join():
	""" This mapper join all the required items for task 3 together and sorted into descending order

	Input data format: place_url_1 \t {tag_top_1 = count} ... {tag_top_10 = count}

					   locality_name \t photocount

	Output data format: locality_name \t photocount \t {tag_top_1 = count} ... {tag_top_10 = count}
	"""

	top_10_locality_name = {}

	# define a dictionary add name-count key-value pair
	with open("part-00000") as f:
		for line in f:
			parts = line.strip().split("\t")
			if len(parts) != 2:
				continue
			locality_name, count = parts[0].strip(), parts[1].strip()
			top_10_locality_name[locality_name] = count


	# define a list for the use of sort
	top_50 = []
	for line in sys.stdin:
		localityName = ''
		items = line.strip().split('\t')

		# check if the data in the right format
		if len(items) != 2:
			continue

		place_url, tags = items[0].strip(), items[1].strip()

		split_place_url = place_url.strip().split('/')
		
		# transform place url into locality name as we use in top 50
		for item in split_place_url[2:]:
			localityName = item + ', ' + localityName
		localityName = localityName + split_place_url[1]
		localityName = localityName.replace('+',' ')


		# combine top 50 and tags
		if localityName in top_10_locality_name:
			count = top_10_locality_name[localityName]
			top_50.append((localityName, int(count), tags))

	# sort in descending order
	sort_top_50 = sorted(top_50, key = lambda x : x[1])
	for name, count, tag in reversed(sort_top_50):
		print(name + '\t' + str(count) + '\t' + tag)




if __name__ == '__main__':
	count_join()