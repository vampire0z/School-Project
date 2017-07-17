#!/usr/bin/python3
# coding: utf-8

import sys

def tag_sort():
	""" This mapper sort top 10 tags for each locality name in descending order

	Input data format: place_url_1 \t {tag_n = count}
					   ...

	Output data format: place_url_1 \t {tag_top_1 = count} ... {tag_top_10 = count}
					    ...
	"""

	top_10_tags = []
	for line in sys.stdin:

		items = line.strip().split('\t')

		# chekc if the data in the right format
		if len(items) != 2:
			continue

		place_url, tags = items[0].strip(), items[1].strip()

		split_Tags = tags.split(',')

		# split tags and ready for sort
		for tag in split_Tags[0:-1]:
			split_tag = tag.strip().split(' = ')
			tag_name, count = split_tag[0].strip(), split_tag[1].strip()
			top_10_tags.append((tag_name, int(count)))

		# sort the tag in descending order and find the top 10
		reversed_top_10_tags = sorted(top_10_tags, key = lambda x : x[1])[-10:]
		top_10_list = ''
		for tag_names, tag_count in reversed(reversed_top_10_tags):
			top_10 = tag_names + '=' + str(tag_count)
			top_10_list += top_10 + ', ' 

		print(place_url + '\t' + top_10_list)
		top_10_tags = []

if __name__ == '__main__':
	tag_sort()