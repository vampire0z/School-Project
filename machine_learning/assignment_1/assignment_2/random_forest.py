import sys
import numpy as np
from sklearn.preprocessing import scale
from sklearn.model_selection import cross_val_score
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report

def unpickle(file):
	import pickle
	#pickle: used to store and load python data object
	# read in bytes
	with open(file, 'rb') as fo:
		dict = pickle.load(fo)
	return dict

def normalise():
	file_name = ['data_batch_1', 'data_batch_2', 'data_batch_3', 'data_batch_4', 'data_batch_5', 'test_batch']
	data_set = []
	print('Loading data')
	for name in file_name:
		data_set.append(unpickle('%s'%('cifar-10-batches-py/'+name)))

	# data_batch_1-5 named as array_1-5 + label_1-5
	# test_batch named as array_6 + label_6
	for x in range(1,7):
		globals()['array_%s'%x] = data_set[x-1].get('data')
		globals()['label_%s'%x] = data_set[x-1].get('labels')

	# Read label data
	label_file_name = 'batches.meta'
	label_dict = unpickle('cifar-10-batches-py/'+label_file_name)
	globals()['label_names'] = label_dict['label_names']

	# group the data together
	X_train = np.concatenate((array_1, array_2, array_3, array_4, array_5))
	y_train = np.concatenate((label_1, label_2, label_3, label_4, label_5))
	X_test, y_test = array_6, label_6

	# run random forest
	random_forest(X_train,y_train,X_test,y_test)

def random_forest(train_data, train_label, test_data, test_label):
	print("Random Forest Classifer")
	clf = RandomForestClassifier(n_estimators = 150, max_features="sqrt", n_jobs = -1)
	clf.fit(train_data, train_label)

	print("Random Forest Classifier Predict:")
	y_true = test_label
	y_pred = clf.predict(test_data)
	target_names = label_names
	#target_names = ['class 0', 'class 1', 'class 2', 'class 3', 'class 4', 'class 5', 'class 6', 'class 7', 'class 8', 'class 9']
	print(classification_report(y_true, y_pred, target_names=target_names))

if __name__ == '__main__':
	normalise()
