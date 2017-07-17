#!src/bin/pyhton3

""" Instructions of running code:
	prepare a folder named input to store the input data
			a folder named output to store the output result
			a folder named temporary to store temporary data we create to use at accuracy analysis
	We are using MacOS Terminal as an example to run our file:
		type: python3 ./NB_tfidf.py
		then the output result will create at location ../output/predicted labels.csv
"""

import pandas as pd
import numpy as np
import operator
import csv


''' Loading data from training data set, and out put with:
    1) a small piece of original data as test data for accuracy test
    2) a csv with each labels total counts
	3) a csv with each labels tf-idf mean
'''
def data_load():
	data = pd.read_csv('../input/training_data.csv',header=None)
	label = pd.read_csv('../input/training_labels.csv',header=None)
	all_data = pd.merge(data,label,on=[0,0])
	train_data = all_data
	count = train_data['1_y'].value_counts()
	count.to_csv('../temporary/count.csv', sep=',')
	# for the accuracy analysis
	# all_data[-2000:].to_csv('temporary/split_data.csv', sep=',',header=None,index=None)
	group_data = train_data.groupby(['1_y']).mean()
	group_data.to_csv('../temporary/group.csv', sep=',')


''' apply naive bayes as a classifier
'''
def NB():
	# load data from temporary output and transform into right format
	data_count = pd.read_csv('../temporary/count.csv',header=None).values
	raw_group_data = pd.read_csv('../temporary/group.csv',header=None).values
	test_data = pd.read_csv('../input/test_data.csv',header=None).values
	#  only for the test
	# test_raw_data = pd.read_csv('../temporary/split_data.csv',header=None).values
	# test_data = test_raw_data[:,:-1]

	# calculate each label's probability P(A) by individual_counts/total_counts store into a dictionary
	sum_count = 0
	for i in data_count:
		sum_count += i[1]
	count_dic = {}	
	for i in data_count:
		count_dic[i[0]] = float(i[1])/float(sum_count)

	# create a dictionary to store each labels tf-idf mean
	group_data = raw_group_data[1:]
	label_dic = {}
	for i in group_data:
		label_dic[i[0]] = i[1:].astype(np.float)

	# a dictionary to store the test data set
	test_dic = {}
	for i in test_data:
		test_dic[i[0]]=i[1:].astype(np.float)


	# create a output file in txt format under the output folder
	# f = open('../output/final.txt','w')
	f = open('../output/predicted_labels.csv','w')
	writer = csv.writer(f, delimiter=',',lineterminator='\n')
	for i in test_dic:
		# create a dictionary to store each test data's probability which is P(Bi|A) i belong to all the test data
		sort_dic = {}
		for j in label_dic:
			a = test_dic.get(i)# tf-idf for test data
			b = label_dic.get(j)# tf-idf for our mean for each label
			# we are using the similarity between tf-idf of test data and the mean tf-idf for all the different labels
			c = 1/(1+np.sqrt((a-b)**2))
			# since the number is too small we take ln for both side of the equations
			# we have a=b*c   -->  In(a) = In(b) + In(c)
			d = np.log10(c).sum()
			sort_dic[j] = d

		# then we find the exactly label and use the equation P(B) = P(B1|A1)P(A1)*P(B2|A)P(A2)*....*P(Bn|An)P(An)
		for j in sort_dic:
			for k in count_dic:
				if j == k:
					sort_dic[j] = sort_dic.get(j)+np.log10(count_dic.get(k))
		# finally we sort the list and choose the biigest one as out prediction label
		sorted_list = sorted(sort_dic.items(), key = operator.itemgetter(1))
		# write a .txt for accuracy test
		# f.write('{} \t {} \n'.format(i,sorted_list[-1][0]))
		writer.writerow([i,sorted_list[-1][0]])
	f.close()

if __name__ == '__main__':
	data_load()
	NB()