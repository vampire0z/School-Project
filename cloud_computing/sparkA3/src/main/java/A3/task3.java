package A3;

import java.util.*;  
import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;
import scala.Tuple3;

/**
 * spark-submit  \
   --class A3.task3 \
   --master yarn \
   --num-executors 10 \
   sparkML.jar \
   hdfs://soit-hdp-pro-1.ucc.usyd.edu.au:8020/user/hzen4403/A3-Spark/task2/ \
   hdfs://soit-hdp-pro-1.ucc.usyd.edu.au:8020/user/hzen4403/A3-Spark/
 */

public class task3{
	public static void main(String[] args) {
		String inputDataPath = args[0];
		String outputDataPath = args[1];
		SparkConf conf = new SparkConf();

		conf.setAppName("Task3");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> task2_data = sc.textFile(inputDataPath+"support.items-size_4_small");

		// read all data from task2 results
		JavaPairRDD<Integer, ArrayList<String>> all = task2_data.mapToPair(s->{
			String[] values = s.split("\t");
			int support = Integer.parseInt(values[0]);
			ArrayList<String> items = new ArrayList<String>();
			for(int i=1; i<values.length; i++){
				items.add(values[i]);
			}
			return new Tuple2<Integer, ArrayList<String>>(support, items);
		});


		// filter out itemset with element bigger than one as set S
		JavaPairRDD<Integer, ArrayList<String>>  onePlus = all.filter(t->{
			return t._2.size()>1;
		});

		// System.out.println(onePlus.collect());
		// System.out.println("===============2=================");
		
		// transform S to list
		ArrayList<Tuple2<Integer, ArrayList<String>>> onePlus_list = 
		new ArrayList<Tuple2<Integer, ArrayList<String>>>(onePlus.collect());

		// System.out.println(onePlus_list);
		// System.out.println("===============original=============");


		// found out the subset of S
		JavaPairRDD<Float, Tuple2<ArrayList<String>, ArrayList<String>>> interaction = all.flatMapToPair(t->{
			ArrayList<String> list_all = t._2;
			ArrayList<Tuple2<Float, Tuple2<ArrayList<String>, ArrayList<String>>>> result_list = 
			new ArrayList<Tuple2<Float, Tuple2<ArrayList<String>, ArrayList<String>>>>();
			Tuple2<Float, Tuple2<ArrayList<String>, ArrayList<String>>> tuple = null;
			for(Tuple2<Integer, ArrayList<String>> oneplus : onePlus_list){
				int support = oneplus._1;
				int plus_size = oneplus._2.size();
				if(list_all.size()<plus_size){
					for (String list_plus : oneplus._2) {
						if(list_all.contains(list_plus)){
							result_list.add(new Tuple2<Float, Tuple2<ArrayList<String>, ArrayList<String>>>((float)support/(float)t._1, new Tuple2<ArrayList<String>, ArrayList<String>>(oneplus._2, list_all)));
							break;
						}
					}
				}
			}
			return result_list.iterator();
		}).filter(t->t._1>=0.6000).sortByKey(false);

		// System.out.println(interaction.collect());

		// toSring and output
		JavaRDD<String> results = interaction.map(t->{
			StringBuilder sb = new StringBuilder();
			String delima = "\t";
			Float confidence = t._1;
			ArrayList<String> r = t._2._2;
			ArrayList<String> s = t._2._1;
			ArrayList<String> s_r = new ArrayList<String>();
			for(String item1:s){
				if(!r.contains(item1)){
					s_r.add(item1);
				}
				
			}
			
			sb.append(r).append(delima);
			sb.append(s_r).append(delima);
			sb.append(confidence);

			return sb.toString();
		});
		// System.out.println("===============final step=============");
		// System.out.println(results.collect());

		results.saveAsTextFile(outputDataPath+"task3/confidence-small");
		sc.close();

	}
}
