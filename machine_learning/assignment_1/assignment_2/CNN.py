#!src/bin/python3

import numpy as np
import matplotlib.pyplot as plt

import keras
from keras.datasets import cifar10
from keras.models import Sequential
from keras.layers import Dense, Dropout, Activation, Flatten
from keras.layers import Conv2D, MaxPooling2D

# loading the training and test dataset
(training_set_data, training_set_labels), (test_data, test_labels) = cifar10.load_data()

# transform the data type into float32
X_train = training_set_data.astype('float32')
X_test = test_data.astype('float32')
print(X_train.shape)
# Normalise training data; 
# Convert class vectors (lables) to binary class matrices.
X_train, y_train = X_train/255, training_set_labels
X_test, y_test = X_test/255, test_labels

# initial and print the size of data
nb_classes = 10
Y_train = keras.utils.to_categorical(y_train, nb_classes)
Y_test = keras.utils.to_categorical(y_test, nb_classes)
print(X_train.shape[0], 'train samples')
print(X_test.shape[0], 'test samples')
print(X_test.shape[1], 'test dimensions')
print(Y_train.shape), 'Y_train.shape' # (40000, 10) length = nb_classes = 10

# build the hidden layers
model = Sequential()
model.add(Conv2D(32, (3, 3), padding ='same', activation = 'relu', input_shape=X_train.shape[1:]))
model.add(Dropout(0.2))
model.add(Conv2D(32, (3, 3), activation = 'relu'))
model.add(MaxPooling2D(pool_size = (2,2)))

# increase convolution kernels to 64
model.add(Conv2D(64, (3, 3), padding='same', activation = 'relu'))
model.add(Dropout(0.2))
model.add(Conv2D(64, (3, 3), activation = 'relu'))
model.add(MaxPooling2D(pool_size=(2, 2)))

# to 128
model.add(Conv2D(128, (3, 3), padding='same', activation = 'relu'))
model.add(Dropout(0.2))
model.add(Conv2D(128, (3, 3), activation = 'relu'))
model.add(MaxPooling2D(pool_size=(2, 2)))

# dense and activation layer
model.add(Flatten())
model.add(Dropout(0.2))
model.add(Dense(1024))
model.add(Activation('relu'))
model.add(Dropout(0.2))
model.add(Dense(512))
model.add(Dropout(0.2))
model.add(Dense(nb_classes))
model.add(Activation('softmax'))

# using SGD optimizers
opt = keras.optimizers.SGD(lr=0.01, momentum=0.9, decay=0.01/30, nesterov=False)
model.compile(loss='categorical_crossentropy', optimizer=opt, metrics=['accuracy'])

# train
print('Fitting model')
epochs = 30
print(epochs, 'epoches')
model.fit(X_train, Y_train, 
        batch_size = 32, nb_epoch = 30, 
        validation_data=(X_test, Y_test),
        shuffle=True)

# predict
print('Evalutating model')
score = model.evaluate(X_test, Y_test, verbose = 1)

print('Score: %1.3f' % score[0])
print('Accuracy: %1.3f' % score[1])