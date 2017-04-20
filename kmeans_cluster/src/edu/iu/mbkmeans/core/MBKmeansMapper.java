package edu.iu.mbkmeans.core;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.CollectiveMapper;
import org.apache.log4j.Logger;

import edu.iu.harp.example.DoubleArrPlus;
import edu.iu.harp.partition.Partition;
import edu.iu.harp.partition.Table;
import edu.iu.harp.resource.DoubleArray;
import edu.iu.mkmeans.common.KMeansConstants;

public class MBKmeansMapper extends CollectiveMapper<String, String, Object, Object> {

	private static final Logger LOGGER = Logger.getLogger(MBKmeansMapper.class);

	private int dimensions;
	private int iteration;
	private int miniBatchSize;
	private final List<String[]> batched_data_set = new ArrayList<>();
	private final Random random = new Random(System.currentTimeMillis());
	private Table<DoubleArray> centroidTable;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		LOG.info("start setup" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime()));
		long startTime = System.currentTimeMillis();
		Configuration configuration = context.getConfiguration();
		dimensions = configuration.getInt(KMeansConstants.VECTOR_SIZE, 20);
		iteration = configuration.getInt(KMeansConstants.NUM_ITERATONS, 1);
		this.miniBatchSize = configuration.getInt(KMeansConstants.MIN_BATCH_SIZE, (dimensions / 2));
		long endTime = System.currentTimeMillis();
		LOG.info("config (ms) :" + (endTime - startTime));
	}

	protected void mapCollective(KeyValReader reader, Context context) throws IOException, InterruptedException {
		LOG.info("Start collective mapper.");
		long startTime = System.currentTimeMillis();
		List<String> pointFiles = new ArrayList<String>();
		while (reader.nextKeyValue()) {
			String key = reader.getCurrentKey();
			String value = reader.getCurrentValue();
			LOG.info("Key: " + key + ", Value: " + value);
			pointFiles.add(value);
		}
		Configuration conf = context.getConfiguration();
		if (isMaster()) {
			LOGGER.info("Master is loading centroids ....... ");
			this.centroidTable = this.loadCentroids(conf);
		}
		this.broadcastCentroids(centroidTable);
		LOGGER.info("Total number of workers - " + this.getNumWorkers());

		LOG.info("Total iterations in master view: " + (System.currentTimeMillis() - startTime));
	}

	private Table<DoubleArray> loadCentroids(Configuration config) throws IOException {
		Table<DoubleArray> cenroidTable = new Table<>(0, new DoubleArrPlus());
		Path cPath = new Path(config.get(KMeansConstants.CFILE));
		FileSystem fs = FileSystem.get(config);
		FSDataInputStream in = fs.open(cPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line = "";
		String[] vector = null;
		int partitionId = 0;
		while ((line = br.readLine()) != null) {
			vector = line.split("\\s+");
			if (vector.length != dimensions) {
				System.out.println("Errors while loading centroids .");
				System.exit(-1);
			} else {
				double[] aCen = new double[dimensions + 1];

				for (int i = 0; i < dimensions; i++) {
					aCen[i] = Double.parseDouble(vector[i]);
				}
				aCen[dimensions] = 0;
				Partition<DoubleArray> ap = new Partition<DoubleArray>(partitionId,
						new DoubleArray(aCen, 0, dimensions + 1));
				cenroidTable.addPartition(ap);
				partitionId++;
			}
		}
		return cenroidTable;
	}

	private void broadcastCentroids(Table<DoubleArray> cenTable) throws IOException {
		// broadcast centroids
		boolean isSuccess = false;
		try {
			isSuccess = broadcast("main", "broadcast-centroids", cenTable, this.getMasterID(), false);
		} catch (Exception e) {
			LOG.error("Fail to bcast.", e);
		}
		if (!isSuccess) {
			throw new IOException("Fail to bcast");
		}
	}

}