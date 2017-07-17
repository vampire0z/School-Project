#!/usr/bin/python3

import sys

def multi_mapper():
	"""
	Records from task1's JoinFile:
	input format of task1's JoinFile: 
					place_url \t place_type_id \t tags
	Output format: 
					place_url_1#1 \t tag
					place_url_1#1 \t tag1 
					place_url_1#1 \t tag2 
					place_url_1#1 \t tag3 
					place_url_2#1 \t tag1
					place_url_2#1 \t tag2

	
	Records format of task2's Top50 File:
		locality_name \t photocount
		i.e Sydney, NSW, Astralia \t 281
	
	Input format: 	place_url_1 \t count 
				  	place_url_1 \t count
	Output format: 
					place_url_1#0 \t count
					place_url_1#0 \t count
					place_url_1#0 \t count
					place_url_2#0 \t count
	"""
	

	for line in sys.stdin:
		parts = line.strip().split("\t")
		# read JOinFile
		if len(parts) == 3:
			place_url, place_type_id, tags = parts[0].strip(), parts[1].strip(), parts[2].strip()

			# reformat the negihbourhood place url to locality place url when type id is 22
			if place_type_id == '22':
				locality_place_url = ""
				splited_place_url = place_url.strip().split('/')
				# since it contains '/'
				for item in splited_place_url[1:-1]:
					locality_place_url = locality_place_url + '/' + item
				#print(place_url, locality_place_url)

				# print url and related tags
				taglist = tags.split(' ')
				for tag in taglist:
					print(locality_place_url + "#1\t" + tag) 

			elif place_type_id == '7':
				# print url and related tags
				taglist = tags.split(' ')
				for tag in taglist:
					print(place_url + "#1\t" + tag)
		
		# read Top50 file
		elif len(parts) == 2:
			locality_place_url = ""
			localityName, count = parts[0].strip(), parts[1].strip()
			#localityName = parts[0].strip()
			split_localityName = localityName.strip().split(', ')

			for word in reversed(split_localityName):
				locality_place_url = locality_place_url + '/' + word.strip()

			locality_place_url = locality_place_url.replace(' ','+')
			print(locality_place_url + "#0\t" + count)



if __name__ == '__main__':
	multi_mapper()
