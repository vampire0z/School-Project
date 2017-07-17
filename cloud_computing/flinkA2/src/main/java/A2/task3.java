package A2;

import java.util.*;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.common.operators.Order;


/* Running instruction */

/* mvn clean install -Pbuild-jar
 * flink run -m yarn-cluster -yn 3 \
  -c A2.task3 \
  target/ml-1.0-SNAPSHOT.jar \
  --cytometry-dir hdfs:///share/cytometry \
  --k 10 \
  --output hdfs:///user/xwan6774/A2-Flink/task3/large-clearOutiler-text_clusterID.count.x.y.z-K_10
 */

public class task3 {

	public static void main(String[] args) throws Exception {
		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);
		// set up execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		env.getConfig().setGlobalJobParameters(params); // make parameters available in the web interface


		String cytometryDir = params.getRequired("cytometry-dir");
		if (cytometryDir.charAt(cytometryDir.length()-1) != '/') {
			cytometryDir = cytometryDir+'/';
		}

		// ======================= Read the file and run filter ======================
		// 1. Read measure data under 'large' floder
		// (sample, FSC-A, SSC-A, SCA1, CD11b, Ly6C)
		DataSet<Tuple6<String, Integer, Integer, Double, Double, Double>> measureData = 
			env.readCsvFile(cytometryDir+"large")	// read csv file under large folder
				.ignoreFirstLine()						// ignore header
				.includeFields("11100011000100000")		// include FSC-A, SSC-A, 'SCA1', 'CD11b', 'Ly6C' fields
				.types(String.class, Integer.class, Integer.class, Double.class, Double.class, Double.class);

		// 2. Filter the measurement data with FSC-A or SSC-A values outside the range (1,150000):
		// we only need SCA1, CD11b, Ly6C, use Map():
		// (sample, SCA1, CD11b, Ly6C)
		DataSet<Point3D> points = 
			measureData
				.filter(measureTuple -> {
					String sample = measureTuple.f0;
					int FSCAvalue = measureTuple.f1, SSCAvalue = measureTuple.f2;

					int lb = 1, ub = 150000;	// lower boundary, upper boudary
					return (FSCAvalue >=lb && FSCAvalue <=ub) 
						&& (SSCAvalue >=lb && SSCAvalue <=ub);
				})
				.map(measureTuple -> {
					Double SCA1value = measureTuple.f3, CD11bvalue = measureTuple.f4, Ly6Cvalue = measureTuple.f5;
					return new Point3D(Ly6Cvalue, CD11bvalue, SCA1value);
				}); 		

		// ==================== Generate Random Clusterid ================== 
		// 3. Get randomly generated k centroids 
		// 		by passing points3D (waiting for clustering points) as given selection set
		DataSet<Centroid> centroids = generateCentroids(params, env, points);


		// ==================== First apply K-means Method ==========================
		// 4.1 Set number of bulk iterations for KMeans algorithm
		final int numIters = 10;
		IterativeDataSet<Centroid> loop = centroids.iterate(numIters);

		// 4.2 Implement K-means
		// 	- 1. Map(): Compute closest centroid for each point via call SelectNearestCenter() function
		// 		and broadcast set "centroids"
		// 		> (centroid, point)
		// 	- 2. Map(): count and sum point coordinates for each centroid
		// 		> (clusterid, Point, 1)
		// 	- 3. GroupBy(): Group by centroidID
		// 	- 4. Reduce(): Combines a group of points into a single centroidID by repeatedly combining two elements into one
		// 		> (clusterID, coordinate sum, point count)
		// 	- 5. Map(): Generate new centroid set via computing average value of x,y,z of all points in same cluster
		// 		> (centroid)
		DataSet<Centroid> newCentroids = points
			.map(new SelectNearestCenter()).withBroadcastSet(loop, "centroids")				// (centroid, point)
			.map(tuple -> new Tuple3<Integer, Point3D, Long>(tuple.f0.id, tuple.f1, 1L))	// (centroidID, point, 1)
			.groupBy(0)		// Group by centroidID (cluster id)
			.reduce((tuple1, tuple2) -> new Tuple3<Integer, Point3D, Long>(tuple1.f0, tuple1.f1.add(tuple2.f1), tuple1.f2 + tuple2.f2))
			.map(tuple -> new Centroid(tuple.f0, tuple.f1.div(tuple.f2))); 

		// 4.3 Feed new centroids back into next iteration and close the iteration via closeWith()
		DataSet<Centroid> finalCentroids = loop.closeWith(newCentroids);	
		
		// 4.4 Assign points to stage 1 final clusters
		// 		> (Centroid, point)
		DataSet<Tuple2<Centroid, Point3D>> clusteredPoints = points
			.map(new SelectNearestCenter()).withBroadcastSet(finalCentroids, "centroids");


		// =========================== This is the dataset with reduced count of original data ==========================
		// 4.5.1 Count the total valid dataset number
		// 		which is 90%  of the original dataset
		DataSet<Tuple2<Centroid, Integer>> centroid_valid_count = clusteredPoints
			.map(tuple -> new Tuple2<Centroid, Integer>(tuple.f0, 1))
			.groupBy(0)
			.sum(1)    // count
			.map(tuple -> new Tuple2<Centroid, Integer>(tuple.f0, tuple.f1*9/10));
		// 4.5.2 join the valid dataset count with points in stage 1 final clusters
		DataSet<Tuple3<Centroid, Point3D, Integer>> groupby_dataset = centroid_valid_count
			.join(clusteredPoints)
			.where(0)
			.equalTo(0)
			.projectFirst(0)
			.projectSecond(1)
			.projectFirst(1);


		// ================== centroid ID, point locations, reduced count number, euclidean distance ==================
		// 4.5.3 Add the euclidean distance into the tuple
		// 		Sorted by centroid ID
		// 		Soerted ascending oder by euclidean distance
		DataSet<Tuple4<Integer, Point3D, Integer, Double>> update_dataset = groupby_dataset
			.map(t-> {
				Double x = t.f0.x;
				Double y = t.f0.y;
				Double z = t.f0.z;
				Point3D c = new Point3D(x,y,z);
				return new Tuple4<Integer, Point3D, Integer, Double>(t.f0.id, t.f1, t.f2, t.f1.euclideanDistance(c));
			})
			.sortPartition(0, Order.ASCENDING) 		// locally sort by centroidID
			.sortPartition(3, Order.ASCENDING); 	// locally sort by distance


		// ================== Apply residual error filter to find the data with big error =====================
		// 4.6 Apply filter
		// 		ID \t point_location \t count \t euclidean_distance
		DataSet<Tuple4<Integer, Point3D, Integer, Double>> reduce_dataset = update_dataset
			.filter(t -> {
				return residualErrorFilter(t.f0) <= t.f2;
			});

		// ================== Retry the K-means Method =============================
		// 5.1 Format the updated points locations
		DataSet<Point3D> updata_points = reduce_dataset
			.map(tuple -> new Point3D(tuple.f1.x, tuple.f1.y, tuple.f1.z));

		// 5.2 Redefine the centroids and loop
		DataSet<Centroid> new_centroids = generateCentroids(params, env, updata_points);
		IterativeDataSet<Centroid> new_loop = new_centroids.iterate(numIters);

		// 5.3 Iteration and run K-means
		DataSet<Centroid> update_newCentroids = points
			.map(new SelectNearestCenter()).withBroadcastSet(new_loop, "centroids")				// (centroid, point)
			.map(tuple -> new Tuple3<Integer, Point3D, Long>(tuple.f0.id, tuple.f1, 1L))	// (centroidID, point, 1)
			.groupBy(0)		// Group by centroidID (cluster id)
			.reduce((tuple1, tuple2) -> new Tuple3<Integer, Point3D, Long>(tuple1.f0, tuple1.f1.add(tuple2.f1), tuple1.f2 + tuple2.f2))
			.map(tuple -> new Centroid(tuple.f0, tuple.f1.div(tuple.f2)));

		// 5.4 Feed new centroids back into next iteration and close the iteration via closeWith()
		DataSet<Centroid> update_finalCentroids = new_loop.closeWith(update_newCentroids);
		// 5.5 Assign points to final clusters
		DataSet<Tuple2<Centroid, Point3D>> update_clusteredPoints = points
			.map(new SelectNearestCenter()).withBroadcastSet(update_finalCentroids, "centroids");

		//  ================= clusterID \t number_of_measurements \t Ly6C \t CD11b \t SCA1  =================== 
		// 6. Format the result as text
		DataSet<String> textData_ClusteridCountCoordinate = update_clusteredPoints
			.map(tuple-> new Tuple2<Centroid, Integer>(tuple.f0, 1)) // (Centroid, 1)
			.groupBy(0)		// group by clusterID
			.sum(1)		// compute count of points in cluster by sum aggregation
			.map(tuple-> {
				int centroidID = tuple.f0.id;
				int pointsCount = tuple.f1; 	// the size of the cluster
				Double x = tuple.f0.x, y = tuple.f0.y, z = tuple.f0.z;
				String clusterCoordinate = String.format("%.6f", x) + "\t" + String.format("%.6f", y) + "\t" + String.format("%.6f", z);
				return new String(centroidID + "\t" + pointsCount + "\t" + clusterCoordinate);
			});

		// ======================= Output and end ===============================
		// 7. End the program by writing the output!
        if(params.has("output")) {
            // Write the result as text
            textData_ClusteridCountCoordinate.writeAsText(params.get("output"));
            env.execute("Task3");
        } else {
            // Always limit direct printing as it requires pooling all resources into the driver.
            System.err.println("No output location specified; printing first 20.");
            textData_ClusteridCountCoordinate.print();
        }

	}



	// ============================ This is a UDF to filter out the top 10% big residual error data ================
	static int previousID = 1;		// Global variable to store the last tuple's cluster id
	static int lineCounter = 0;		// Global variable to store the line count
	/* This function Return the currnt cluster's line count value */
	public static Integer residualErrorFilter(Integer idNum){
		int currentID = idNum;
		// If in the same cluster, then update the line counter
		// otherwise, re-initialize the linecounter to 1
		if(currentID == previousID){
			lineCounter++;
		}
		else{
			lineCounter = 1;
		}
		// update the centroid ID to current centroid ID
		previousID = currentID;
		return lineCounter;
	}


	// ============================= This function to generate centroids =========================
	// This function Randomly generate clutsers with id and coordinates 
	public static DataSet<Centroid> generateCentroids(ParameterTool params, ExecutionEnvironment env, DataSet<Point3D> points) {
		DataSet<Centroid> centroids;

		Set<Centroid> centroidSet = new HashSet<Centroid>();	// initialise the centroid set	
		Set<Point3D> selectedPs = new HashSet<Point3D>();		// initialise the selected poits set
		Random r = new Random();								// initialise random r
		//if (params.has("k")) {
		int k = params.getInt("k", 10); // get the input k value, the default value is 10

		// Randomly pick k points
		while (selectedPs.size() < k){
			Double rangeMin = -1.0;
			Double rangeMax = 6.0;
			Double x = rangeMin + (rangeMax - rangeMin) * r.nextDouble();			
			Double y = rangeMin + (rangeMax - rangeMin) * r.nextDouble();
			Double z = rangeMin + (rangeMax - rangeMin) * r.nextDouble();
			x = Math.round(x*1000000d)/1000000d; // to 6 decimal places
			y = Math.round(y*1000000d)/1000000d; // to 6 decimal places
			z = Math.round(z*1000000d)/1000000d; // to 6 decimal places
			Point3D p = new Point3D(x, y, z);
			selectedPs.add(p);
		}

		// Add clutser id to centro points, and store it to centroid set
		Iterator<Point3D> iter = selectedPs.iterator();
		int id = 1;
		while(iter.hasNext()) {	// construct centroid, assign an id as label
			Centroid c = new Centroid(id++, iter.next());
			centroidSet.add(c);
		}
		// 5. Creates a DataSet from given Collection centroidSet
		centroids = env.fromCollection(centroidSet);
		return centroids;
	}	



	// ========================== This is a class to select nearest centers for each input point ==========================
	public static final class SelectNearestCenter extends RichMapFunction<Point3D, Tuple2<Centroid, Point3D>> {
	//public static final class SelectNearestCenter extends RichMapFunction<Point3D, Tuple2<Integer, Point3D>> {
		private Collection<Centroid> centroidsSet;

		// Read the centroid values from a broadcast variable, write into a collection
		public void open(Configuration parameters) throws Exception {
			this.centroidsSet = getRuntimeContext().getBroadcastVariable("centroids");
		}
		// public Tuple2<Integer, Point3D> map(Point3D point) throws Exception{
		public Tuple2<Centroid, Point3D> map(Point3D point) throws Exception{
			double minDistance = Double.MAX_VALUE; // the maximum value a double can represent
			int closestCentroidId = -1;
			Centroid closestCentroid = new Centroid();
			// check all cluster centers
			for (Centroid centroid : centroidsSet) {
				// Compute distance 
				double distance = point.euclideanDistance(centroid);
				// Update the nearest clusterID if necessary
				if (distance < minDistance) {
					minDistance = distance;
					//closestCentroidId = centroid.id;
					closestCentroid = centroid;
				}
			}
			//return new Tuple2<Integer, Point3D>(closestCentroidId, point);
			return new Tuple2<Centroid, Point3D>(closestCentroid, point);
		}
	}

}


