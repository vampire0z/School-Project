#!/bin/bash

if [ $# -lt 3 ]; then
    echo "Invalid number of parameters!"
    echo "Usage: ./reduceside_join_driver.sh /share/place.txt /share/photo/ [Assignment1 or output location that must be same in task2.sh and task3.sh]"
    exit 1
fi

hdfs dfs -rm -r -f ""$3"/Task1"

hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar \
-D stream.num.map.output.key.fields=2 \
-D map.output.key.field.separator=# \
-D mapreduce.partition.keypartitioner.options=-k1,1 \
-D mapreduce.job.maps=15 \
-D mapreduce.job.reduces=10 \
-D mapreduce.job.name='Associate photo with locality place' \
-file reduceside_join_mapper.py \
-mapper reduceside_join_mapper.py \
-file reduceside_join_reducer.py \
-reducer reduceside_join_reducer.py \
-input $1 \
-input $2 \
-output ""$3"/Task1/place-photo-joinFile" \
-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner




hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar \
-D stream.num.map.output.key.fields=2 \
-D mapreduce.partition.keypartitioner.options=-k1,1 \
-D mapreduce.job.maps=10 \
-D mapreduce.job.reduces=10 \
-D mapreduce.job.name='locality name with photo count' \
-file counter_mapper.py \
-mapper counter_mapper.py \
-reducer counter_reducer.py -file counter_reducer.py \
-input  ""$3"/Task1/place-photo-joinFile"/part-* \
-output ""$3"/Task1/Task1_Result_Locality_Photo+Count" \
-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner

