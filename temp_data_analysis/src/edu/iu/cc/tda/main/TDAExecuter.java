package edu.iu.cc.tda.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.iu.cc.tda.mapred.TemperatureDataMapper;
import edu.iu.cc.tda.mapred.TemperatureDataReducer;

public class TDAExecuter {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: TDAExecuter <input path> <output path>");
			System.exit(-1);
		}
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(TDAExecuter.class);
		job.setJobName("Max temperature");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(TemperatureDataMapper.class);
		job.setReducerClass(TemperatureDataReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
