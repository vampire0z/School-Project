###Running Instruction

For predicting test data, **NB-tfidf.py** is the only file should be run.  
   
   **accuracy.py** used for the analysis of our 
   algorithm



**NB_tfidf.py**

>
prepare a folder named input to store the input data
			a folder named output to store the output result
			a folder named temporary to store temporary data we create to use at accuracy analysis  
			
We are using MacOS Terminal as an example to run our file:

```
>>> python3 ./NB_tfidf.py
```
then the output result will be create at location ../output/predicted labels.csv 

**accuracy.py**
>
This test is only for our training data accuracy test, make sure there are file exist at  
output/final.txt,  
temporary/split_data.csv

```
>>> python3 ./accuracy.py

```