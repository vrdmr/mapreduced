package org.vmeru.research.clustering.error;

public class DistanceMetric {

	public static double calculateEuclideanDistance(Double[] dataPoint, Double[] centroid) {
		double innerSum = 0;
		for (int i = 0; i < centroid.length; i++) {
			innerSum += Math.pow((centroid[i] - dataPoint[i]), 2.0);
		}
		return Math.sqrt(innerSum);
	}
}
