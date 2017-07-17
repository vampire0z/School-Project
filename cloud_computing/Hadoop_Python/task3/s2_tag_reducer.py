#!/usr/bin/python3
# coding: utf-8

import sys

def read_map_output(file):
	""" Return an iterator for key, value pair extracted from file (sys.stdin).
	Input format:  key \t value
	Output format: (key, value)
	"""
	for line in file:
		yield line.strip().split("\t", 1)


def contain_location_tag(tag,split_url):
	contain_not_valid_tag = False
	for location in split_url:
		if location.lower().replace('+','') == tag.lower():
			contain_not_valid_tag = True
	return contain_not_valid_tag

def contain_year_tag(tag,split_url):
	contain_not_valid_tag = False
	#encode tag avoid non-ASCII character problem
	tag = tag.encode('ascii','ignore')
	# Filter tag with year
	if tag.strip().isdigit():
		value = float(tag)
		if value >=1900 and value <=2020:
			contain_not_valid_tag = True
	return contain_not_valid_tag


def tag_reducer():
	""" This reducer select and filter tags then return tag-frequency information

	Input data format: place_url_1 \t tag_1
						place_url_1 \t tag_1
						place_url_1 \t tag_2
						...
						place_url_50 \t tag_10

	Output data format: place_url_1 \t {tag_n = count}
						...
	"""
	current_url = ''
	tag_count = {}

	for url, tag in read_map_output(sys.stdin):
		# define a filter to find invalid tags out
		split_url = url.strip().split('/')
		# Filter tag with location information in url
		if contain_location_tag(tag,split_url) == True:
			continue
		# Filter tag with year
		if contain_year_tag(tag,split_url) == True:
			continue

		# Count the tag and print the output
		if current_url != url:
			if current_url != "":
				output = current_url + "\t"

				# count tags and output
				for tg, count in tag_count.items():
					output += '{} = {},'.format(tg, count)
				print(output.strip())
			# Reset current url and tag count
			current_url = url
			tag_count = {}

		tag_count[tag] = tag_count.get(tag, 0) + 1


	# print last url
	if current_url != "":
		# same filter as before
		split_last_url = current_url.strip().split('/')
		
		# if tag not contaion location information and year information
		# then print the output
		if contain_location_tag(tag,split_last_url)==False and contain_year_tag(tag,split_last_url)==False:
			output = current_url + "\t"
			for tag, count in tag_count.items():
				output += '{} = {},'.format(tag, count)
			print(output.strip())


if __name__ == '__main__':
	tag_reducer()







