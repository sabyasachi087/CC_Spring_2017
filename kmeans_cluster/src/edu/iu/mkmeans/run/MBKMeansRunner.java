package edu.iu.mkmeans.run;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.iu.mbkmeans.core.MBKmeansMapper;
import edu.iu.mkmeans.common.KMeansConstants;

public class MBKMeansRunner extends Configured implements Tool {

	private final String jobId;

	private MBKMeansRunner() {
		this.jobId = String.valueOf(System.currentTimeMillis()).substring(5);
	}

	public static void main(String[] argv) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MBKMeansRunner(), argv);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {

		if (!this.validate(args)) {
			return -1;
		}
		MBKArgProperty prop = new MBKArgProperty();
		prop.localDataFilePath = args[0];
		prop.dimensions = Integer.parseInt(args[1]);
		prop.iterations = Integer.parseInt(args[2]);
		prop.batchSize = Integer.parseInt(args[3]);
		prop.localCentroidFilePath = args[4];
		Job mbkJob = this.configureMBK(getConf(), prop);
		if (mbkJob.waitForCompletion(true)) {
			System.out.println("***************************************************MBKMeans Ended***********************************");
		}
		return 0;
	}

	private boolean validate(String[] args) {
		if (args.length < 5) {
			System.err.println("Usage: edu.iu.mkmeans.run.MBKMeansRunner <local_data_file_path>   "
					+ "<dimensions_of_each_record> <nbr_of_iterations> <batch_size>  <local_centroid_file_path> ");
			ToolRunner.printGenericCommandUsage(System.err);
			return false;
		}
		return true;
	}

	private Job configureMBK(Configuration config, MBKArgProperty prop) throws Exception {
		FileSystem fs = FileSystem.get(config);
		Path workDir = new Path(KMeansConstants.WORK_DIR + this.jobId);
		Path inputDataDir = new Path(workDir, KMeansConstants.INPUT_DIR);
		fs.copyFromLocalFile(new Path(prop.localDataFilePath), inputDataDir);
		Path centroidDataDir = new Path(workDir, KMeansConstants.CENTROID_DIR);
		fs.copyFromLocalFile(new Path(prop.localCentroidFilePath), centroidDataDir);
		Path centroidFilePath = fs.listFiles(centroidDataDir, false).next().getPath();

		Path outDir = new Path(workDir, KMeansConstants.OUTPUT_DIR);

		Job job = Job.getInstance(config, "mbkmeans_job_" + this.jobId);
		Configuration jobConfig = job.getConfiguration();
		if (fs.exists(outDir)) {
			fs.delete(outDir, true);
		}
		FileInputFormat.setInputPaths(job, inputDataDir);
		FileOutputFormat.setOutputPath(job, outDir);

		System.out.println("Centroid File Path: " + centroidFilePath.toString());
		jobConfig.set(KMeansConstants.CFILE, centroidFilePath.toString());
		jobConfig.set(KMeansConstants.JOB_ID, this.jobId);
		jobConfig.setInt(KMeansConstants.NUM_ITERATONS, prop.iterations);
		jobConfig.setInt(KMeansConstants.DIMENSIONS, prop.dimensions);
		jobConfig.setInt(KMeansConstants.MIN_BATCH_SIZE, prop.batchSize);
		job.setInputFormatClass(FileInputFormat.class);
		job.setJarByClass(MBKMeansRunner.class);
		job.setMapperClass(MBKmeansMapper.class);
		return job;
	}

	private static final class MBKArgProperty {
		private String localCentroidFilePath;
		private Integer dimensions;
		private Integer iterations;
		private Integer batchSize;
		private String localDataFilePath;
	}

}
