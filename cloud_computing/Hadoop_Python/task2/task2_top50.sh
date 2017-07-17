#!/bin/bash

if [ $# -lt 1 ]; then
    echo "Invalid number of parameters!"
    echo "Usage: ./place_photo_count_driver.sh [Assignment1 or output location that must be same in task1.sh and task3.sh]"
    exit 1
fi

hdfs dfs -rm -r -f ""$1"/Task2"

hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar \
-D mapreduce.job.maps=3 \
-D mapreduce.job.reduces=1 \
-D mapreduce.job.name='Top50 Photo Count' \
-file task2_count_sort_mapper.py \
-mapper task2_count_sort_mapper.py \
-file task2_count_sort_reducer.py \
-reducer task2_count_sort_reducer.py \
-input ""$1"/Task1/Task1_Result_Locality_Photo+Count"/part* \
-output ""$1"/Task2/Task2_Top50" \
