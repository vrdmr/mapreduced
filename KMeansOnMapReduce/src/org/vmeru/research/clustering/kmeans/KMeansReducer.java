package org.vmeru.research.clustering.kmeans;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * @author Varad Meru, Orzota, Inc.
 */
public class KMeansReducer extends MapReduceBase implements
		Reducer<IntWritable, Text, IntWritable, Text> {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
	 */
	@Override
	public void reduce(IntWritable key, Iterator<Text> values,
			OutputCollector<IntWritable, Text> output, Reporter arg3)
			throws IOException {

		// Initialising the variables for the map method
		long numberOfPoints = 0;
		double[] centroid = null;
		Text value = values.next();
		String valueToString = value.toString();
		List<String> dataPointListRepresentation = Arrays.asList(valueToString.split("\t"));
		Double[] dataPointDoubleArrayRepresentation = convertListToDoubleArray(dataPointListRepresentation);
		centroid = new double[dataPointDoubleArrayRepresentation.length];

		while (value != null) {

			// Counter to maintain the number of datapoints in the cluster (with same cluster_id key) - used for finding the mean
			numberOfPoints++;

			// Adding the datapoints into centoid data-structure
			for (int i = 0; i < dataPointDoubleArrayRepresentation.length; i++) {
				centroid[i] += dataPointDoubleArrayRepresentation[i];
			}

			// Converting a line of Text (Serealised String in Hadoop) to Double[]. This is one data-point.
			if (values.hasNext()) {
				// doing the parsing and conversion into double[]
				value = values.next();
				valueToString = value.toString();
				dataPointListRepresentation = Arrays.asList(valueToString.split("\t"));
				dataPointDoubleArrayRepresentation = convertListToDoubleArray(dataPointListRepresentation);
			} else {
				// End condition
				value = null;
			}
		}

		// Get the average of the sum of datapoints indices.
		for (int i = 0; i < centroid.length; i++) {

			centroid[i] = centroid[i]/numberOfPoints;
		}

		// TODO - Need to check the output formatting of the array.
		/*
		 * Need to change the formatting as per your own, or write a small
		 * utility outside this MapReduce code which will do the changes for you
		 *
		 * Reference Java Code:
		 * String []array = new String[3];
		 * array[0] = "Australia";
		 * array[1] = "Denmark";
		 * array[2] = "Itlay";
		 * System.out.println(Arrays.toString(array));
		 *
		 * Output: Java Code:
		 * [Australia, Denmark, Itlay]
		 */
		output.collect(key, new Text(centroid.toString()));
	}

	/**
	 * TODO - Verify the Casting
	 *
	 * @param valuesStringArray
	 * @return
	 */
	private Double[] convertListToDoubleArray(List<String> valuesStringArray) {
		Double[] dataPoints = (Double[]) valuesStringArray.toArray(new Double[valuesStringArray.size()]);

		return dataPoints;
	}
}