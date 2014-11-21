package org.vmeru.research.clustering.kmeans;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


/**
 * @author Varad Meru, Orzota, Inc.
 */
public class KMeansOnMapReduce {

	// Main method
	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(KMeansOnMapReduce.class);
	
		try {
			DistributedCache.addCacheFile(new URI(args[3]), conf);
		} catch (URISyntaxException e1) {
			e1.printStackTrace();
		}
		
		// Name of the Job
		conf.setJobName("KMeans_MapReduceVerison");

		// Output data formats for Mapper
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(Text.class);

		// Final Output data formats (output of reducer)
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		// Formats of the Data Type of Input and output
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		// Specifying the Mapper and Reducer Class
		conf.setMapperClass(KMeansMapper.class);
		conf.setReducerClass(KMeansReducer.class);
		
		// Specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		// Setting the job configuration
		client.setConf(conf);
		try {
			// Running Job with configuration conf.
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}