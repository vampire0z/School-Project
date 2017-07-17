#!src/bin/python3
"""Instruction of running accuracy test
   This test is only for our training data accuracy test, make sure there are file exist at  output/final.txt, temporary/split_data.csv
   Run: python3 ./accuracy.py
"""

import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, confusion_matrix, precision_recall_fscore_support
import operator

""" We are using sklearn library to calculate our confusion_matrix, accuracy, precision, recall, and f-score
"""

def accuracy():
	# load the data
	predict = pd.read_csv('../output/final.txt', sep='\t', header=None).values
	actual_raw = pd.read_csv('../temporary/split_data.csv', sep=',', header=None).values
	actual = actual_raw[:,[0,-1]]

	# assign data into different dictionary, get the predict data as y_pred, actual data as y_true
	predict_dic={}
	for i in predict:
		predict_dic[i[0]] = i[1]
	d = sorted(predict_dic.items(), key=operator.itemgetter(0))
	y_pred = []
	for i in d:
		y_pred.append(i[1])
	# print(y_pred)

	actual_dic = {}
	for i in actual:
		actual_dic[i[0]] = i[1]
	e = sorted(actual_dic.items(), key=operator.itemgetter(0))
	y_true = []
	for i in e:
		y_true.append(i[1])
	# print(y_true)

	# using sklearn library to do this
	print(accuracy_score(y_true, y_pred))
	print(precision_recall_fscore_support(y_true,y_pred,average='micro'))


	# Plot Confusion Matrix
	print(confusion_matrix(y_true,y_pred))
	df_cm = pd.DataFrame(confusion_matrix(y_true,y_pred))
	plt.figure(figsize = (10,7))
	plt.title('Confusion matrix of the NB classifier')
	plt.xlabel('Predicated')
	plt.ylabel('True')
	sn.heatmap(df_cm, annot=True)
	plt.show()

	# Print classification performance
	accuracy,precision,recall,fmeausre = performace(df_cm)
	print("Label | Accuracy | Precision | Recall | F1-measure") 
	for i in range(30):	
		print(str(i)+'\t|'+ str(accuracy[i])+'\t|' + str(precision[i]) + '\t|' + str(recall[i]) + '\t|' + str(fmeausre[i]))

def performace(c_m):
	FP = c_m.sum(axis=0) - np.diag(c_m)
	FN = c_m.sum(axis=1) - np.diag(c_m)
	TP = np.diag(c_m)
	TN = c_m.values.sum() - (FP + FN + TP)	

	# Precision 
	p = TP/(TP+FP)
	# Recall 
	r = TP/(TP+FN)
	# F1-measure
	f = 2*p*r/(p+r)

	# Overall accuracy
	ACC = (TP+TN)/(TP+FP+FN+TN)
	return ACC,p,r,f


if __name__ == '__main__':
	accuracy()