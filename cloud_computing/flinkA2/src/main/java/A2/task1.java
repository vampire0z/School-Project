package A2;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.flink.configuration.GlobalConfiguration;

/**
 * Input Data:
 * 	experiments.csv (only 2.2KB)
 * 	
 * Input fromat:
 * 	sample, date, experiment, day, subject, kind, instrument, researchers
 * 
 * Sample Data:
 * 	WNVBMaSMPL1,9.10.2015,WNVBMa,0,M1,bone marrow,LSR-II,Baltimore;Darwin
 * 	WNVBMaSMPL9,13.10.2015,WNVBMa,2,M5,bone marrow,LSR-II,Gesner
 *
 * Input Data:
 * 	large/measurements_arcsin200_p1.csv (51.84MB)
 * 	large/measurements_arcsin200_p2.csv (51.84MB)
 * 	
 * Input fromat:
 * 	sample, FSC-A, SSC-A, CD48, Ly6G, CD117, SCA1, CD11b, CD150, CD11c, B220, Ly6C,...
 *-------------------------------------------------------------
 * mvn clean install -Pbuild-jar
 * flink run -m yarn-cluster -yn 3 \
  -c A2.task1 \
  target/ml-1.0-SNAPSHOT.jar \
  --cytometry-dir hdfs:///share/cytometry \
  --output hdfs:///user/xwan6774/A2-Flink/task1/text-researcher.count
 */
public class task1 {
	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		String cytometryDir = params.getRequired("cytometry-dir");
		if (cytometryDir.charAt(cytometryDir.length()-1) != '/') {
			cytometryDir = cytometryDir+'/';
		}

		// 1. Read measure data under 'large' floder
		DataSet<Tuple3<String, Integer, Integer>> measureData = 
			env.readCsvFile(cytometryDir+"large")	// read csv file under large folder
				.ignoreFirstLine()						// ignore header
				.includeFields("11100000000000000")		// include first 3 fields
				.types(String.class, Integer.class, Integer.class);

	
		// 1.1 Filter the valid measurement data first:
		// we get: (sample,FSC,SSC);
		// we only need to count the number of measurement,use map:
		// (sample,1)
		DataSet<Tuple2<String, Integer>> validMeasureData = 
			measureData
			.filter(measureTuple -> {
				String sample = measureTuple.f0;
				int FSCAvalue = measureTuple.f1;
				int SSCAvalue = measureTuple.f2;
				int lb = 1, ub = 150000;	// lower boundary, upper boudary
				return (FSCAvalue >=lb && FSCAvalue <=ub) 
					&& (SSCAvalue >=lb && SSCAvalue <=ub);
			})
			.map(tuple -> new Tuple2<String, Integer>(tuple.f0, 1)); 


		// 1.2 Method 1 (more efficient): GroupBY 
		// calculate the sample count
		// (sample, count)
		DataSet<Tuple2<String, Integer>> sampleCount = 
			validMeasureData
				.groupBy(0)	// group by sample
				.sum(1)		// sum all number of valid samples
				.map(tuple -> new Tuple2<String, Integer>(tuple.f0,tuple.f1));


		// 2.1 Read sample researchers data from CSV file experiments.csv
		// (Sample, researchers)
		DataSet<Tuple2<String, String>> sampleResearchers = 
			env.readCsvFile(cytometryDir+"experiments.csv")
				.ignoreFirstLine()				// ignore header
				.includeFields("10000001")		// include the first and the last field
				.types(String.class, String.class);
		
		// 2.2 Use Flat map: Takes researchers list and produces sample for each researcher
		// (sample, researcher)
		DataSet<Tuple2<String, String>> sampleResearcher = 
			sampleResearchers
				.flatMap((tuple, out) -> {
					String sample = tuple.f0;
					// clear white spaces and split by ';'
					String[] researchers = tuple.f1.split(";");	

					for (String researcher : researchers) {
							out.collect(new Tuple2<String, String>(sample, researcher.trim()));	// remove whitesapce
					}
				});
       
        // 3.Join (sample, researcher) with (sample, count)
        // output: (researcher, sample, count )
        DataSet<Tuple3<String, String, Integer>> researcherSampleCount = 
        	sampleResearcher
        		.join(sampleCount)
        		.where(0)
        		.equalTo(0)
        		.projectFirst(1)		// project first field from 1st DataSet
        		.projectSecond(0,1);	// project frist and second field from 2nd Dataset

		// ===============Text: Researcher \t count ===============
		// 4. Format the result as string
        DataSet<String> researcherMeasurecount = 
        	researcherSampleCount
        		.groupBy(0) // group by researcher
        		.sum(2)		// sum aggregation (researcher, count )
        		.map(tuple -> new String(tuple.f0 + "\t" + tuple.f2));

		// End the program by writing the output!
		if (params.has("output")) {
			//researcherMeasurecount.writeAsCsv(params.get("output"));
			researcherMeasurecount.writeAsText(params.get("output"));
			// execute program
			env.execute("researcher - measurement count");
		} else {
			// Always limit direct printing
			// as it requires pooling all resources into the driver.
			System.err.println("No output location specified; Printing result to stdout.");
			researcherMeasurecount.print();
		}

	}
}
