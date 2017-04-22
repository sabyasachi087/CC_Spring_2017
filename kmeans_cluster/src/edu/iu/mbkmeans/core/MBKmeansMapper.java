package edu.iu.mbkmeans.core;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.CollectiveMapper;
import org.apache.log4j.Logger;

import edu.iu.harp.example.DoubleArrPlus;
import edu.iu.harp.partition.Partition;
import edu.iu.harp.partition.Table;
import edu.iu.harp.resource.DoubleArray;
import edu.iu.mkmeans.common.KMeansConstants;

public class MBKmeansMapper extends CollectiveMapper<LongWritable, Text, Text, Text> {

	private static final Logger LOGGER = Logger.getLogger(MBKmeansMapper.class);
	private static final String CONTEXT_NAME = "MINI_BATCH_K_MEANS_CTX";
	private static Integer TABLE_ID = 1;
	private int dimensions;
	private int iteration;
	private int miniBatchSize;
	private Table<DoubleArray> centroidTable;
	private Table<DoubleArray> newCentroidTable = new Table<>(TABLE_ID++, new DoubleArrPlus());
	private final List<String> ORIGINAL_DATA_FILES = new ArrayList<>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		LOGGER.info("start setup" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime()));
		long startTime = System.currentTimeMillis();
		Configuration configuration = context.getConfiguration();
		dimensions = configuration.getInt(KMeansConstants.DIMENSIONS, 20);
		iteration = configuration.getInt(KMeansConstants.NUM_ITERATONS, 1);
		this.miniBatchSize = configuration.getInt(KMeansConstants.MIN_BATCH_SIZE, 10);
		long endTime = System.currentTimeMillis();
		LOGGER.info("config (ms) :" + (endTime - startTime));
	}

	protected void mapCollective(KeyValReader reader, Context context) throws IOException, InterruptedException {
		LOGGER.info("Start collective mapper.");
		long startTime = System.currentTimeMillis();
		List<String> pointFiles = new ArrayList<String>();
		while (reader.nextKeyValue()) {
			Text value = reader.getCurrentValue();
			pointFiles.add(value.toString());
		}
		ORIGINAL_DATA_FILES.addAll(pointFiles);
		Configuration conf = context.getConfiguration();
		if (isMaster()) {
			LOGGER.info("Master is loading centroids ....... ");
			this.centroidTable = this.loadCentroids(conf);
		}
		this.broadcastPointTable(centroidTable);
		LOGGER.info("Total number of workers - " + this.getNumWorkers());
		LOGGER.info("Worker Id  - " + this.getSelfID());
		this.executeMBKMeans(pointFiles);
		if (this.isMaster())
			this.flushCentroids(context);
		LOGGER.info("Total iterations in master view: " + (System.currentTimeMillis() - startTime));
	}

	private void executeMBKMeans(List<String> datapoints) throws IOException {
		Table<DoubleArray> randomDataPoints;
		while (iteration-- > 0) {
			this.newCentroidTable = new Table<>(TABLE_ID++, new DoubleArrPlus());
			randomDataPoints = this.getBatchDataPoints(datapoints);
			this.updatePartition(randomDataPoints);
			allgather(CONTEXT_NAME, "ag_datapoint_iter_" + iteration, randomDataPoints);
			int workerId = this.getSelfID();
			DoubleArray updatedCentroid;
			for (int id = 0; id < this.centroidTable.getPartitionIDs().size(); id++) {
				if (id == workerId) {
					updatedCentroid = this.recalculateCentroids(this.centroidTable.getPartition(id).get(),
							randomDataPoints, id);
					this.newCentroidTable.addPartition(new Partition<DoubleArray>(id, updatedCentroid));
					workerId = workerId + this.getNumWorkers();
				}
			}
			allgather(CONTEXT_NAME, "ag_centroid_iter_" + iteration, this.newCentroidTable);
			this.centroidTable = this.newCentroidTable;
		}
	}

	private DoubleArray recalculateCentroids(DoubleArray centroid, Table<DoubleArray> dataPoints, int partitionId) {
		Integer centroidPartitionId;
		Map<Integer, Integer> centroidCountMap = new HashMap<>();
		Double learningRate;
		for (Partition<DoubleArray> dataPoint : dataPoints.getPartitions()) {
			centroidPartitionId = (int) dataPoint.get().get()[dimensions];
			if (centroidPartitionId == partitionId) {
				if (centroidCountMap.get(centroidPartitionId) == null) {
					centroidCountMap.put(centroidPartitionId, 1);
				} else {
					centroidCountMap.put(centroidPartitionId, centroidCountMap.get(centroidPartitionId) + 1);
				}
				learningRate = (1d / centroidCountMap.get(centroidPartitionId));
				this.applyGradientStep(dataPoint.get(), centroid, learningRate);
			}
		}
		return centroid;
	}

	private void applyGradientStep(DoubleArray dataPoints, DoubleArray centroid, Double learningRate) {
		Double nlr = 1 - learningRate;
		int indx = 0;
		for (Double centroidPoint : centroid.get()) {
			centroid.get()[indx++] = centroidPoint * nlr;
		}
		indx = 0;
		for (Double dataPoint : dataPoints.get()) {
			if (indx == dimensions) {
				break;
			} else {
				centroid.get()[indx] = centroid.get()[indx] + dataPoint * nlr;
				indx++;
			}
		}
	}

	private void updatePartition(Table<DoubleArray> dataPoints) {
		for (Partition<DoubleArray> dataPoint : dataPoints.getPartitions()) {
			double minDist = -1;
			double tempDist = 0;
			int nearestPartitionID = -1;
			for (Partition<DoubleArray> ap : this.centroidTable.getPartitions()) {
				DoubleArray aCentroid = (DoubleArray) ap.get();
				tempDist = this.getEucleideanDistance(dataPoint.get(), aCentroid, dimensions);
				if (minDist == -1 || tempDist < minDist) {
					minDist = tempDist;
					nearestPartitionID = ap.id();
				}
			}
			// update the partition id
			dataPoint.get().get()[dimensions] = nearestPartitionID;
		}
	}

	private Table<DoubleArray> loadCentroids(Configuration config) throws IOException {
		Table<DoubleArray> cenroidTable = new Table<>(TABLE_ID++, new DoubleArrPlus());
		Path cPath = new Path(config.get(KMeansConstants.CFILE));
		FileSystem fs = FileSystem.get(config);
		FSDataInputStream in = fs.open(cPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line = "";
		String[] vector = null;
		int partitionId = 0;
		while ((line = br.readLine()) != null) {
			// LOGGER.info("Reading Centroid >> " + line);
			vector = line.split("\\s+");
			if (vector.length != dimensions) {
				System.out.println("Errors while loading centroids .");
				System.exit(-1);
			} else {
				double[] aCen = new double[dimensions];

				for (int i = 0; i < dimensions; i++) {
					aCen[i] = Double.parseDouble(vector[i]);
				}
				Partition<DoubleArray> ap = new Partition<DoubleArray>(partitionId,
						new DoubleArray(aCen, 0, dimensions));
				cenroidTable.addPartition(ap);
				partitionId++;
			}
		}
		return cenroidTable;
	}

	private void broadcastPointTable(Table<DoubleArray> cenTable) throws IOException {
		boolean isSuccess = false;
		try {
			isSuccess = broadcast(CONTEXT_NAME, "bc_pointTable", cenTable, this.getMasterID(), false);
		} catch (Exception e) {
			LOGGER.error("Fail to bcast.", e);
		}
		if (!isSuccess) {
			throw new IOException("Fail to bcast");
		}
	}

	private Table<DoubleArray> getBatchDataPoints(List<String> datapoints) {
		Table<DoubleArray> dpTable = new Table<>(TABLE_ID++, new DoubleArrPlus());
		int batchSize = ((this.miniBatchSize % this.getNumWorkers()) + this.miniBatchSize) / this.getNumWorkers();
		Collections.shuffle(datapoints);
		Partition<DoubleArray> paritionDataPoint;
		String[] vectors;
		double[] points;
		int pntCount = 0;
		Integer partitionId;
		for (int i = 0; i < batchSize; i++) {
			vectors = datapoints.get(i).split("\\s+");
			points = new double[dimensions + 1];
			partitionId = null;
			for (String vector : vectors) {
				if (partitionId == null) {
					partitionId = Integer.parseInt(vector);
				} else {
					points[pntCount++] = Double.parseDouble(vector);
				}
			}
			points[dimensions] = -1;
			pntCount = 0;
			paritionDataPoint = new Partition<DoubleArray>(partitionId, new DoubleArray(points, 0, dimensions + 1));
			dpTable.addPartition(paritionDataPoint);
		}
		LOGGER.info("Batched table created successfully .... ");
		return dpTable;
	}

	@SuppressWarnings("unused")
	private Table<DoubleArray> getDataPointsTable() {
		Table<DoubleArray> dpTable = new Table<>(TABLE_ID++, new DoubleArrPlus());
		int batchSize = ((this.miniBatchSize % this.getNumWorkers()) + this.miniBatchSize) / this.getNumWorkers();
		Partition<DoubleArray> paritionDataPoint;
		String[] vectors;
		double[] points;
		int pntCount = 0;
		Integer partitionId;
		for (int i = 0; i < batchSize; i++) {
			vectors = ORIGINAL_DATA_FILES.get(i).split("\\s+");
			points = new double[dimensions + 1];
			partitionId = null;
			for (String vector : vectors) {
				if (partitionId == null) {
					partitionId = Integer.parseInt(vector);
				} else {
					points[pntCount++] = Double.parseDouble(vector);
				}
			}
			points[dimensions] = -1;
			pntCount = 0;
			paritionDataPoint = new Partition<DoubleArray>(partitionId, new DoubleArray(points, 0, dimensions + 1));
			dpTable.addPartition(paritionDataPoint);
		}
		LOGGER.info("Batched table created successfully .... ");
		return dpTable;
	}

	// find Euclidean distance.
	private double getEucleideanDistance(DoubleArray aPoint, DoubleArray otherPoint, int vectorSize) {
		double dist = 0;
		for (int i = 0; i < vectorSize; i++) {
			dist += Math.pow(aPoint.get()[i] - otherPoint.get()[i], 2);
		}
		return Math.sqrt(dist);
	}

	private void flushCentroids(Context context) {
		StringBuilder output = new StringBuilder();
		for (Partition<DoubleArray> ap : this.centroidTable.getPartitions()) {
			double res[] = ap.get().get();
			output.append(ap.id()).append("\t");
			for (int i = 0; i < dimensions; i++)
				output.append(res[i] + "\t");
			output.append("\n");
		}
		try {
			context.write(null, new Text(output.toString()));
		} catch (IOException e) {
			e.printStackTrace();
			LOGGER.error(e);
		} catch (InterruptedException e) {
			LOGGER.error(e);
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unused")
	private void printTable(Table<DoubleArray> dataTable) {
		for (Partition<DoubleArray> ap : dataTable.getPartitions()) {

			double res[] = ap.get().get();
			System.out.print("ID: " + ap.id() + ":");
			for (int i = 0; i < res.length; i++)
				System.out.print(res[i] + "\t");
			System.out.println();
		}
	}

}