package A2;

import java.util.*;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;


/* mvn clean install -Pbuild-jar
 * flink run -m yarn-cluster -yn 3 \
  -c A2.task2 \
  target/ml-1.0-SNAPSHOT.jar \
  --cytometry-dir hdfs:///share/cytometry \
  --k 10 \
  --output hdfs:///user/xwan6774/A2-Flink/task2/large-text_clusterID.count.x.y.z-K_10
 */

public class task2 {

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

		// 1. Read measure data under 'large' floder
		// (sample, FSC-A, SSC-A, SCA1, CD11b, Ly6C)
		DataSet<Tuple6<String, Integer, Integer, Double, Double, Double>> measureData = 
			env.readCsvFile(cytometryDir+"large")		// read csv file under samll or large folder
				.ignoreFirstLine()						// ignore header
				.includeFields("11100011000100000")		// include FSC-A, SSC-A, 'SCA1', 'CD11b', 'Ly6C' fields
				.types(String.class, Integer.class, Integer.class, Double.class, Double.class, Double.class);

		
		// 2. Filter the measurement data with FSC-A or SSC-A values outside the range (1,150000): (sample, FSC-A, SSC-A, SCA1, CD11b, Ly6C)
		// we only need SCA1, CD11b, Ly6C, use Map(): -> (SCA1, CD11b, Ly6C) 
		// return type is Point3D: -> (Point3D)
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
		

		// ===============This is Random Clusterid===============
		// 3. Get randomly generated k centroids 
		// by passing points3D (waiting for clustering points) as given selection set
		DataSet<Centroid> centroids = generateCentroids(params, env, points);


		// =============== Bulk Iteration for K-Means =======================
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

		// ===============This is  Final Centroids ===============
		// 4.3 Feed new centroids back into next iteration and close the iteration via closeWith()
		DataSet<Centroid> finalCentroids = loop.closeWith(newCentroids);	
		
		// 4.4 Assign points to final clusters
		// > (Centroid, point)
		DataSet<Tuple2<Centroid, Point3D>> clusteredPoints = points
			.map(new SelectNearestCenter()).withBroadcastSet(finalCentroids, "centroids");

		// ======== clusterID \t number_of_measurements \t Ly6C \t CD11b \t SCA1  ===============
		// 5. Format the result as text
		DataSet<String> textData_ClusteridCountCoordinate = clusteredPoints
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


		// 6. End the program by writing the output!
        if(params.has("output")) {
            // Write the result as text
            textData_ClusteridCountCoordinate.writeAsText(params.get("output"));
            env.execute("Task2 K-means");
        } else {
            // Always limit direct printing as it requires pooling all resources into the driver.
            System.err.println("No output location specified; printing first 20.");
            //clusteridCountCoordinate.first(20).print();
            textData_ClusteridCountCoordinate.first(20).print();
        }

	}

	/* 
		Randomly generate clutsers with id and coordinates 
	*/
	public static DataSet<Centroid> generateCentroids(ParameterTool params, ExecutionEnvironment env, DataSet<Point3D> points) {
		DataSet<Centroid> centroids = null;							// declare a centroids dataset
	try{
		
		Set<Centroid> centroidSet = new HashSet<Centroid>();	// initialise the centroid set	
		Set<Point3D> selectedPs = new HashSet<Point3D>();		// initialise the selected poits set
		Random r = new Random();								// initialise random r
		//if (params.has("k")) {
		int k = params.getInt("k", 10); // get the input k value, the default value is 10

		// 1. Map () the points to coordinates
		DataSet<Tuple3<Double, Double, Double>> coordinates = points
			.map(tuple-> new Tuple3<Double, Double, Double>(tuple.x, tuple.y, tuple.z));
		// 2. Extract min and max value of each coordinate
		List<Double> minX = coordinates.minBy(0).map(t->new Double(t.f0)).collect(), maxX = coordinates.maxBy(0).map(t->new Double(t.f0)).collect();	// X: SCA1 field
		List<Double> minY = coordinates.minBy(1).map(t->new Double(t.f1)).collect(), maxY = coordinates.maxBy(1).map(t->new Double(t.f1)).collect();	// Y: CD11b field
		List<Double> minZ = coordinates.minBy(2).map(t->new Double(t.f2)).collect(), maxZ = coordinates.maxBy(2).map(t->new Double(t.f2)).collect();	// Z: Ly6C field
		Double minx = minX.get(0), maxx = maxX.get(0);		
		Double miny = minY.get(0), maxy = maxY.get(0);
		Double minz = minZ.get(0), maxz = maxZ.get(0);
		
		// 3. Randomly pick k points
		while (selectedPs.size() < k){
			Double x = minx + (maxx - minx) * r.nextDouble();
			Double y = miny + (maxy - miny) * r.nextDouble();
			Double z = minz + (maxz - minz) * r.nextDouble();

			Point3D p = new Point3D(x, y, z);
			selectedPs.add(p);
		}

		// 4. Add clutser id to centro points, and store it to centroid set
		Iterator<Point3D> iter = selectedPs.iterator();
		int id = 1;
		while(iter.hasNext()) {	// construct centroid, assign an id as label
			Centroid c = new Centroid(id++, iter.next());
			centroidSet.add(c);
		}
		// 5. Creates a DataSet from given Collection centroidSet
		centroids = env.fromCollection(centroidSet);
		//return centroids;
	}catch(Exception ex){ /*swallow*/}
	finally{ return centroids;}
	}	

	/* 
		Select the nearest center for each points 
	*/
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



