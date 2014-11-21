package org.vmeru.research.clustering.rkmeans;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Varad Meru, Orzota, Inc.
 */
public class RKMeansReducer extends Reducer<IntWritable, Text, Text, Text> {

	private double wupper;
	private double wlower;
	private String delimeter = "$$$";
	private String upper = "U";
	private String lower = "L";

	@Override
	public void reduce(IntWritable key, java.lang.Iterable<Text> values,
			Context context) throws IOException, InterruptedException {

		// Initialising the variables for the reduce method
		int numberOfPointsInUpper = 0;
		int numberOfPointsInLower = 0;
		double[] centroid = null;
		double[] totalPointsSumLower = null;
		double[] totalPointsSumUpper = null;
		String boundType = null;

		String valueToString = null;
		List<String> dataPointListRepresentation = null;
		Double[] dataPointDoubleArrayRepresentation = null;

		// Running the loop only for one iteration to get some metadat about the
		// number of dimensions of the data points.
		for (Text text : values) {
			valueToString = text.toString();
			// boundType = delimetedValue.get(0);

			StringTokenizer stringTokenizer = new StringTokenizer(
					valueToString, delimeter);
			stringTokenizer.nextToken();

			// Splitting the value string text by the delimiter ($$$ in our
			// case) and the splitting the string at index = 1, by the tab
			// delimeter.
			dataPointListRepresentation = Arrays.asList(stringTokenizer
					.nextToken().split("\t"));

			// Converting the Double List to Double []
			dataPointDoubleArrayRepresentation = convertListToDoubleArray(dataPointListRepresentation);

			// Initialisation done for totalPointsSumUpper and
			// totalPointsSumLower
			totalPointsSumLower = new double[dataPointDoubleArrayRepresentation.length];
			totalPointsSumUpper = new double[dataPointDoubleArrayRepresentation.length];

			// Initialisation done for centroid
			centroid = new double[dataPointDoubleArrayRepresentation.length];
			break;
		}

		// Parsing the values for each cluster
		for (Text text : values) {
			valueToString = text.toString();
			StringTokenizer stringTokenizer = new StringTokenizer(
					valueToString, delimeter);

			boundType = stringTokenizer.nextToken();

			// The point is converted from Text to a List<Double> representation
			dataPointListRepresentation = Arrays.asList(stringTokenizer
					.nextToken().split("\t"));

			// List<Double> to Double [].
			dataPointDoubleArrayRepresentation = convertListToDoubleArray(dataPointListRepresentation);

			// If the value part contains 'U' as a prefix, the data-object
			// dimensions will be added to the upper-bound values aggregating
			// array
			if (boundType.equalsIgnoreCase(upper)) {
				for (int i = 0; i < dataPointDoubleArrayRepresentation.length; i++) {
					totalPointsSumUpper[i] += dataPointDoubleArrayRepresentation[i];
					numberOfPointsInUpper++;
				}
				// If the value part contains 'L' as a prefix, the data-object
				// dimensions will be added to the lower-bound values
				// aggregating array
			} else if (boundType.equalsIgnoreCase(lower)) {
				for (int i = 0; i < dataPointDoubleArrayRepresentation.length; i++) {
					totalPointsSumLower[i] += dataPointDoubleArrayRepresentation[i];
					numberOfPointsInLower++;
				}
			} else {
				try {
					throw new Exception("Formatting error. Please check");
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			context.write(new Text(key + "-" + "member"), text);
		}

		// Calculation the centroid for the 'rough' cluster.

		if (numberOfPointsInLower != 0 && numberOfPointsInUpper == 0) {
			for (int i = 0; i < centroid.length; i++) {
				centroid[i] = (totalPointsSumLower[i] / numberOfPointsInLower);
			}
		} else if (numberOfPointsInLower == 0 && numberOfPointsInUpper != 0) {
			for (int i = 0; i < centroid.length; i++) {
				centroid[i] = (totalPointsSumUpper[i] / numberOfPointsInUpper);
			}
		} else {
			for (int i = 0; i < centroid.length; i++) {
				centroid[i] = wlower
						* (totalPointsSumLower[i] / numberOfPointsInLower)
						+ wupper
						* (totalPointsSumUpper[i] / numberOfPointsInUpper);
			}
		}

		context.write(new Text(key + "-" + "centroid"), new Text(centroid[0] + "\t" + centroid[1]));
	}

	/**
	 * TODO - Verify the Casting
	 *
	 * @param valuesStringArray
	 * @return
	 */
	private Double[] convertListToDoubleArray(List<String> valuesStringArray) {
		Double[] dataPoints = new Double[valuesStringArray.size()];
		for (int i = 0; i < valuesStringArray.size(); i++) {
			dataPoints[i] = Double.parseDouble(valuesStringArray.get(i));

		}
		return dataPoints;
	}

	protected void setup(
			org.apache.hadoop.mapreduce.Reducer<IntWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();

		wlower = Double.parseDouble(configuration.get("rkmeans.w.lower"));
		wupper = Double.parseDouble(configuration.get("rkmeans.w.upper"));
	}

}