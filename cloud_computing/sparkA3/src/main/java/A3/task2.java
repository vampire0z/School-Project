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
   --class A3.task2 \
   --master yarn \
   --num-executors 60 \
   sparkML.jar \
   hdfs://soit-hdp-pro-1.ucc.usyd.edu.au:8020/share/genedata/small/ \
   hdfs://soit-hdp-pro-1.ucc.usyd.edu.au:8020/user/hzen4403/A3-Spark/
 */


public class task2{

	public static void main(String[] args) {
		String inputDataPath = args[0];
		String outputDataPath = args[1];
		SparkConf conf = new SparkConf();

		conf.setAppName("Task2");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// read GEO.txt and PatientData without header
		JavaRDD<String> patientData = sc.textFile(inputDataPath+"PatientMetaData.txt").filter(line->{
			return !line.contains("id");
		});
		JavaRDD<String> geoData = sc.textFile(inputDataPath+"GEO.txt").filter(line->{
			return !line.contains("patientid");
		});


		// patientid, geneid, expression_value
		// Use filter to filter the geodata that gene >1,250,000
		JavaPairRDD<String, Integer> geo_Data = geoData.mapToPair(s->{
			String[] values = s.split(",");
			String patientID = values[0];
			int geneNo = Integer.parseInt(values[1]);
			Float expressionValue= Float.parseFloat(values[2]);
			return new Tuple2<String, Tuple2<Integer, Float>>(patientID, new Tuple2<Integer, Float>(geneNo, expressionValue));
		}).filter(t->{
			int geneNo = t._2._1;
			Float expressionValue = t._2._2;
			return expressionValue > 1250000;
		}).mapToPair(t->{
			return new Tuple2<String, Integer>(t._1, t._2._1); // patientid, geneid
		});


		// s: id, age, gender, postcode, diseases, drug_response
		// flatMapToPair is used because one patient can have multiple dieases
		// id, disease1; id, disease2;...
		JavaPairRDD<String, String> patientWithCancer = patientData.flatMapToPair(s->{
			String[] values = s.split(",");
			String patientID = values[0];
			
			ArrayList<Tuple2<String, String>> results = new ArrayList<Tuple2<String, String>>();
			if (values.length >=6) {
				String[] diseases = values[4].split(" "); // split by space
				for (String disease: diseases) {
					results.add(new Tuple2<String, String>(patientID, disease));
				}
			}
			return results.iterator();
		}).filter(t->{
			// filter records with disease: breast-cancer, prostate-cancer, pancreatic-cancer, leukemia, or lymphoma
			String disease = t._2;
			return disease.contains("breast-cancer")
				|| disease.contains("prostate-cancer") 
				|| disease.contains("pancreatic-cancer")
				|| disease.contains("leukemia")
				|| disease.contains("lymphoma");
		});

		//patientDiease.take(30).foreach(line->println(line));


		// (patientID,Cancertype) (patientID, geneid with expression_value>1250000)
		// Join: -> (patientID, (Cancertype, geneid))
		// MapToPair: -> (patientID, geneid)
		JavaPairRDD<String, Integer> cancerPatientID = patientWithCancer.join(geo_Data).mapToPair(t->{
		//JavaPairRDD<String, Integer> cancerPatientID = patientWithCancer.join(geo_Data).mapPartitionsToPair(t->{
			String patientID = t._1;
			Integer geneid = t._2._2;
			return new Tuple2<String, Integer>(patientID, geneid);
		});
//		System.out.println("==============TEST 1============");

		// set the initial support threshold with 30% minimal support
		long num_patientWithCancer = cancerPatientID.sortByKey().keys().distinct().count();
		double minSupport = 0.3;
		long threshold = (long)(num_patientWithCancer*minSupport);
		
		// 1. Computer frequent set with size 1
		JavaPairRDD<ArrayList<Integer>, Integer> c1 = cancerPatientID.mapToPair(t->{
			ArrayList<Integer> itemset = new ArrayList<Integer>();
			itemset.add(t._2);
			return new Tuple2<ArrayList<Integer>, Integer>(itemset, 1);
		}).reduceByKey((v1,v2)-> v1 + v2).filter(t-> t._2 >= threshold);
		//JavaPairRDD<ArrayList<Integer>, Integer> c1 = c1_1.reduceByKey((v1,v2)-> v1 + v2).filter(t-> t._2 >= threshold);

		// 2. Compute all Frequent Set
		int frequent = 3;	// set the max itemset_size as iteration number

		Set<Integer> tmpEleSet = new HashSet<Integer>();
		JavaPairRDD<ArrayList<Integer>, Integer> finalUnion = c1;
		JavaPairRDD<String, ArrayList<Integer>> patientGenesset = null;

		for (int i = 2; i<=frequent; i++) {
			System.out.println("\n Item Set size:"+i);
			
			final int f = i;
			Set<Integer> eleSet = transferEleSet(c1.collect()); 	// Original elements set: extract keys (items) in c1
			if (i == 2) {
				tmpEleSet = eleSet;
			}
			Set<Integer> nextEleSet = tmpEleSet;
//			System.out.println("Next Genes Set: \n"+nextEleSet +"\n");

			if (nextEleSet.isEmpty()) {
				System.out.println("Next gene set is EMPTY!\n");
				break;
			}
			if (nextEleSet.size() < f) {
				System.out.println("Next gene set is not suitable!\n");
				break;
			}
			
			//1. Original: PatientiD \t GeneID
			JavaPairRDD<String, Integer> filteredPatientGeneID= cancerPatientID.filter(t->{
				return nextEleSet.contains(t._2); 	// filter the patientIDGeneID that is contained in itemset
			});
//			System.out.println("1. Filtered Patient GeneID\n" + filteredPatientGeneID.take(50));

			if (i==2) {
				System.out.println("1.1. i = 2\n Use original: PatientiD \t GeneSet\n");
				// 1.2. Convert original to: PatientiD \t GeneSet
				patientGenesset = filteredPatientGeneID.mapToPair(t->{
					ArrayList<Integer> elements = new ArrayList<Integer>();
					elements.add(t._2);
					return new Tuple2<String, ArrayList<Integer>>(t._1, elements);
				});
				//System.out.println("Patient + ItemSet Genes: \n" + patientGenesset.take(50) +"\n");
			}else{System.out.println("1.2. Use previous: PatientiD \t GeneSet\n");}
			

			System.out.print("\n2. Start Do self Join!\n");
			//2. Do self Join: generate new (PatientID GeneID)
			//Join: (patient10,({9}，14)）, (patient10，({9}，21)) 
			//MapToPair: (patient10，({9，14})）,(patient10，({9，21})) 
			
			//---------2. Use mapToPair: ---------------------
			JavaPairRDD<String, ArrayList<Integer>> newPatientGeneID = patientGenesset.join(filteredPatientGeneID).mapToPair(t->{
				//Tuple2<ArrayList<Integer>, Integer> itemsetInteger= t._2;
				String patientID = t._1;
				ArrayList<Integer> itemset = new ArrayList<Integer>();
				for (Integer item : t._2._1) {
					itemset.add(item);
				}
				int gene = t._2._2; 			// new gene
				if (!itemset.contains(gene)) { 	// If gene not exist in the set, then add it to set
					itemset.add(gene);
				}// else If gene is already in the set, do nothing
				Collections.sort(itemset);
				return new Tuple2<String, ArrayList<Integer>>(patientID, itemset);
			}).filter(t->{ // filter the array that the length is exactly = frequent value
				return t._2.size() == f;	// t._2 = geneArr
			}).distinct();
//			System.out.println("2. newPatientGeneID Join + MapToPair + filter+ distinct \n" + newPatientGeneID.take(50));

			// -------3. v2 use MapToPair----------
			JavaPairRDD<ArrayList<Integer>, Integer> newGeneSetCount = newPatientGeneID.mapToPair(t->{
				ArrayList<Integer> itemset = new ArrayList<Integer>();
				for (Integer item : t._2) {
					itemset.add(item);
				}
				return new Tuple2<ArrayList<Integer>, Integer>(itemset, 1);
			}).reduceByKey((v1,v2)->v1+v2).filter(t->t._2 >= threshold);			

			// Union the result
			finalUnion = finalUnion.union(newGeneSetCount);			

			// 1. Update the  temporary element set used for filtering
			tmpEleSet = transferEleSet(newGeneSetCount.collect());
			// System.out.print("temporary Eleset: \n" + tmpEleSet);
			// 2. Update the  patientGenesset set used for join
			patientGenesset = newPatientGeneID;

		}

		// 1. Reverse the key value --> Support Count \t ItemSet
		// 2. SortByKey in desending order
		// 3. GroupByKey to aggregate
		// 4. Format the results as: support \t item1 \t item2 \t itemn
		//JavaPairRDD<Integer, ArrayList<Integer>> supportValue_Genes = finalUnion.mapToPair(t->{
		JavaRDD<String> supportValue_Genes = finalUnion.mapToPair(t->{
			ArrayList<Integer> itemset = t._1;
			int count = t._2;
			return new Tuple2<Integer, ArrayList<Integer>>(count, itemset);
		}).sortByKey(false).map(t->{
			StringBuilder sb = new StringBuilder();
			String delima = "\t";
			int support = t._1;
			sb.append(support).append(delima);
			ArrayList<Integer> items = t._2;
			//sb.append(items).append(delima);
			for (Integer item : items) {
				sb.append(item).append(delima);
			}
			return sb.toString();
		});
		//supportValue_Genes.saveAsTextFile(outputDataPath+"task2/support-test.v.final");
		supportValue_Genes.saveAsTextFile(outputDataPath+"task2/support.items-size_4_small");

		sc.close();

	}

	private static Set<Integer> transferEleSet(List<Tuple2<ArrayList<Integer>, Integer>> tuples){
		Set<Integer> eleSet = new HashSet<Integer>();
		for (Tuple2<ArrayList<Integer>, Integer> tuple : tuples) {
			//ArrayList<Integer> elements = tuple._1;
			for (Integer ele: tuple._1) {
				eleSet.add(ele);
			}
		}
		return eleSet;
	}

}

