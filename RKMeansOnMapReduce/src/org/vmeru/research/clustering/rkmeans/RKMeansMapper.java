package org.vmeru.research.clustering.rkmeans;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.vmeru.research.clustering.error.DistanceMetric;

/**
 * It finds the distances between the data points and the centroids. The output
 * of the mapper is of the form <code><cluster_number, data_point></code>, where
 * cluster_number represents the cluster whose centroid is the nearest to the
 * data point.
 *
 * {@link RKMeansMapper} extends {@link Mapper}
 *
 * @author Varad Meru, Orzota, Inc.
 */
public class RKMeansMapper
		extends
		org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, IntWritable, Text> {

	private Path[] localFiles = null;
	private List<Double[]> centroids = null;
	private double threshold;
	private String delimeter = "$$$";
	private String upper = "U";
	private String lower = "L";

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {

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
		Double[] distanceFromCentroids = calculateDistancesFromCentroids(
				dataPointDoubleArrayRepresentation, centroids);
		List<Integer> nearestCentroids = findClusters(distanceFromCentroids);

		if (nearestCentroids.size() != 0) {
			if (nearestCentroids.size() == 1) {
				// Spilling the nearest Centroid
				context.write(new IntWritable(nearestCentroids.get(0)),
						new Text(lower + delimeter + value));
			} else {
				// Spilling the nearest Centroids, as upper bound centroids.
				for (Integer integer : nearestCentroids) {
					context.write(new IntWritable(integer), new Text(upper
							+ delimeter + value));
				}
			}
		}
	}

	protected void setup(
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {

		// Getting the configurtions, used for the cluster.
		Configuration configuration = context.getConfiguration();

		try {
			threshold = Double.parseDouble(configuration
					.get("rkmeans.threshold"));

			centroids = new ArrayList<Double[]>();

			// Getting the files from Distr. Cache
			localFiles = DistributedCache.getLocalCacheFiles(configuration);

			BufferedReader bufferedReader = null;

			/*
			 * Print objects from Cache file
			 */
			for (int i = 0; i < localFiles.length; i++) {

				// Matching the name of the file ("cache"). As the cache file
				// can
				// also be a directory, and thus contain multiple files
				if (localFiles[i].toString().contains("centroids")) {

					String centorid = null;
					bufferedReader = new BufferedReader(new FileReader(localFiles[i].toString()));

					// Converting the String text to an Array of Strings splitted by
					// tab, converting into Double[] and then adding to a list of
					// centroids,which is passed for calculating the distances
					while ((centorid = bufferedReader.readLine()) != null) {
						centroids.add(convertListToDoubleArray(Arrays.asList(centorid.split("\t"))));
					}
				}

			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Calculates the minimum distance from the passed array of distances.
	 *
	 * @param distanceFromCentroid
	 * @return List of Cluster IDs which the data-points is associated with. If
	 *         size = 1, the only a lower bound for the cluster, else in upper
	 *         bounds.
	 */
	private List<Integer> findClusters(Double[] distanceFromCentroid) {
		// Counting starting from 1 for cluster number and not from 0

		List<Integer> clusters = new ArrayList<Integer>();
		int minIndex = 1;

		Double minDistance = distanceFromCentroid[0];
		for (int i = 1; i < distanceFromCentroid.length; i++) {
			if (minDistance > distanceFromCentroid[i]) {
				minDistance = distanceFromCentroid[i];
				// Adjustment for the above counting statement.
				minIndex = i + 1;
			}
		}

		clusters.add(minIndex);
		for (int i = 1; i < distanceFromCentroid.length; i++) {
			if (i != (minIndex - 1)) {
				if (distanceFromCentroid[minIndex - 1]
						/ distanceFromCentroid[i] >= threshold) {
					clusters.add(i + 1);
				}
			}
		}

		return clusters;
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
			i++;
		}

		return distances;
	}

	/**
	 * @param valuesStringArray
	 * @return
	 */
	private Double[] convertListToDoubleArray(List<String> valuesStringArray) {
		Double[] dataPoints = new Double [valuesStringArray.size()];
		for (int i = 0; i < valuesStringArray.size(); i++) {
			dataPoints[i] = Double.parseDouble(valuesStringArray.get(i));

		}
		return dataPoints;
	}
}