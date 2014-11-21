package org.vmeru.research.clustering.kmeans;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.vmeru.research.clustering.error.DistanceMetric;

/**
 * It finds the distances between the data points and the centroids. The output
 * of the mapper is of the form <code><cluster_number, data_point></code>, where
 * cluster_number represents the cluster whose centroid is the nearest to the
 * data point.
 *
 * {@link KMeansMapper} extends {@link MapReduceBase} and implements
 * {@link Mapper}.
 *
 * @author Varad Meru, Orzota, Inc.
 */
public class KMeansMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, IntWritable, Text> {

	private Path[] localFiles = null;
	private List<Double[]> centroids = null;

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * org.apache.hadoop.mapred.MapReduceBase#configure(org.apache.hadoop.mapred
	 * .JobConf)
	 */
	@Override
	public void configure(JobConf job) {
		try {
			String centorid = null;
			centroids = new ArrayList<Double[]>();

			// Getting the files from Distr. Cache
			localFiles = DistributedCache.getLocalCacheFiles(job);

			// Getting the file handler for file in cache
			File file = new File(localFiles[0].getName());
			BufferedReader bufferedReader = new BufferedReader(new FileReader(
					file));

			// Converting the String text to an Array of Strings splited by tab,
			// converting into Double[] and then adding to a list of centroids,
			// which is passed for calculating the distances
			while ((centorid = bufferedReader.readLine()) != null) {

				centroids.add(convertListToDoubleArray(Arrays.asList(centorid
						.split("\t"))));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object,
	 * java.lang.Object, org.apache.hadoop.mapred.OutputCollector,
	 * org.apache.hadoop.mapred.Reporter)
	 */
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, Text> output, Reporter reporter)
			throws IOException {

		// Initializing the variables for the map method
		String valueToString = null;
		Double[] dataPointDoubleArrayRepresentation = null;
		List<String> dataPointListRepresentation = null;

		// Converting a line of Text (Serialized String in Hadoop) to Double[].
		// This is one data-point.
		valueToString = value.toString();
		dataPointListRepresentation = Arrays.asList(valueToString.split("\t"));
		dataPointDoubleArrayRepresentation = convertListToDoubleArray(dataPointListRepresentation);

		// Distances from various centroids. The number of centroids in first
		// cache file would be the input k.
		Double[] distanceFromCentroid = calculateDistancesFromCentroids(
				dataPointDoubleArrayRepresentation, centroids);
		int nearestCentroid = findMinimum(distanceFromCentroid);

		// Spilling the nearestCentroid
		output.collect(new IntWritable(nearestCentroid), value);
	}

	/**
	 * Calculates the minimum distance from the passed array of distances.
	 *
	 * @param distanceFromCentroid
	 * @return clusterID of the closest centroid
	 */
	private int findMinimum(Double[] distanceFromCentroid) {
		// Counting starting from 1 for cluster number and not from 0
		int minIndex = 1;
		Double minDistance = distanceFromCentroid[0];
		for (int i = 1; i < distanceFromCentroid.length; i++) {

			if (minDistance > distanceFromCentroid[i]) {

				minDistance = distanceFromCentroid[i];
				// Adjustment for the above counting statement.
				minIndex = i + 1;
			}
		}

		return minIndex;
	}

	/**
	 * @param dataPoint
	 * @param centroids
	 * @return
	 */
	private Double[] calculateDistancesFromCentroids(Double[] dataPoint,
			List<Double[]> centroids) {
		int i = 0;
		Double[] distances = new Double[centroids.size()];

		for (Double[] centroid : centroids) {
			distances[i] = new Double(
					DistanceMetric.calculateEuclideanDistance(dataPoint,
							centroid));
		}

		return distances;
	}

	/**
	 * @param valuesStringArray
	 * @return
	 */
	private Double[] convertListToDoubleArray(List<String> valuesStringArray) {
		Double[] dataPoints = (Double[]) valuesStringArray
				.toArray(new Double[valuesStringArray.size()]);

		return dataPoints;
	}
}