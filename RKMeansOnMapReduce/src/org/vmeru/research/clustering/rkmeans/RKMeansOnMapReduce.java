package org.vmeru.research.clustering.rkmeans;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Varad Meru, Orzota, Inc.
 */
public class RKMeansOnMapReduce extends Configured implements Tool {

	// Main method
	public static void main(String[] args) throws Exception {

		int exitCode = ToolRunner.run(new Configuration(),new RKMeansOnMapReduce(), args);

		// Exits with a a status update.
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		boolean success = false;

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 5) {
			System.err.println("Usage: RKMeansOnMapReduce.jar <in> <out> <threshold> <w-lower> <w-upper>");
			System.exit(2);
		}

		conf.set("rkmeans.threshold", otherArgs[2]);
		conf.set("rkmeans.w.lower", otherArgs[3]);
		conf.set("rkmeans.w.upper", otherArgs[4]);

		final String NAME_NODE = conf.get("fs.default.name");

		// Absolute path of your cache file.
		final String ABS_PATH_OF_DISTRIBUTED_CACHE_FILE = "/user/varadmeru/centroids.dat";

		// Just a precaution if the properties are not set correctly.
		// Should be set with the format hdfs://<namenode>:<port>
		if (NAME_NODE != null) {
			DistributedCache.addCacheFile(new URI(NAME_NODE+ ABS_PATH_OF_DISTRIBUTED_CACHE_FILE), conf);
		}

		Job job = new Job(conf, "Rough KMeans using Mapreduce");
		job.setJarByClass(RKMeansOnMapReduce.class);

		// Output data formats for Mapper
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		// Final Output data formats (output of reducer)
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Formats of the Data Type of Input and output
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Specifying the Mapper and Reducer Class
		job.setMapperClass(RKMeansMapper.class);
		job.setReducerClass(RKMeansReducer.class);

		// Specifying the paths for input and output dirs
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		// Submitting the job
		success = job.waitForCompletion(true);

		// Returning the success/failure info back to the calling method (main)
		return success ? 0 : 1;
	}
}