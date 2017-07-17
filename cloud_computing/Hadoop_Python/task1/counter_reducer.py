#!/usr/bin/python3

import sys

def read_combiner_output(file):
	""" Return an iterator for key, value pair extracted from file (sys.stdin).
    Input format:  key \t value
    Output format: (key, value)
    """

	for line in file:
		yield line.strip().split("\t",1)

def locality_reducer():

	"""
	e.g.locality_name1, 1
	locality_name1 \t 128
	locality_name1 \t 80
	locality_name2 \t 300
	locality_name2 \t 37
	locality_name3 \t 75
	locality_name3 \t 90

	
	input format: {(locality_name1, 1),(locality_name1, 1),(locality_name2, 1)...}
	output format: 
		locality_name1 	\t count_sum
		locality_name2 	\t count_sum

	"""


	current_locality_name = ""
	photo_count = 0

	for locality_name, count in read_combiner_output(sys.stdin):
		# Check if the current locality name read is the same as the locality name currently being processed
		if current_locality_name != locality_name:

			# If this is the first line which has the defult value ""
			# we do not need to output photo count yet.
			# Otherwise, we need to output the currently being processed locality name and its photo count
			if current_locality_name != "":
				output = "{}\t{}".format(current_locality_name,photo_count)
				print(output.strip())

			
			# Reset the locality name being processed and reset the photo_count value to 0
			current_locality_name = locality_name
			photo_count = 0

		
		# If the current locality name read is the same as the locality name currently being processed
		# process it value addition
		photo_count = photo_count + 1
		#photo_count[locality_name] = photo_count.get(locality_name,0) + 1

	
	# We need to output the locality name - photo count value for the last locality name
	# we only want to do this if the for loop is called.
	if current_locality_name != "":
		output = "{}\t{}".format(current_locality_name,photo_count)
		print(output.strip())



if __name__ == "__main__":
	locality_reducer()