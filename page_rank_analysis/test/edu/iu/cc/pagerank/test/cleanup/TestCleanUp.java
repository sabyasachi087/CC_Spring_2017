package edu.iu.cc.pagerank.test.cleanup;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import edu.iu.cc.pagerank.test.io.InMemoryDataStore;
import indiana.cgl.hadoop.pagerank.helper.CleanupResultsMap;
import indiana.cgl.hadoop.pagerank.helper.CleanupResultsReduce;
import indiana.cgl.hadoop.pagerank.helper.PRWritable;

public class TestCleanUp {

	MapDriver<LongWritable, Text, PRWritable, DoubleWritable> mapDriver;
	ReduceDriver<PRWritable, DoubleWritable, LongWritable, DoubleWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, PRWritable, DoubleWritable, LongWritable, DoubleWritable> mapReduceDriver;

	public void init() {
		CleanupResultsMap crm = new CleanupResultsMap();
		mapDriver = MapDriver.newMapDriver(crm);
		CleanupResultsReduce crr = new CleanupResultsReduce();
		reduceDriver = ReduceDriver.newReduceDriver(crr);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(crm, crr);
	}

	public void testMapReduce() throws IOException {
		this.mapReduceDriver.withAll(InMemoryDataStore.getPageRankOutput()).getConfiguration().setInt("numUrls", 11);
		List<Pair<LongWritable, DoubleWritable>> output = this.mapReduceDriver.run();
		InMemoryDataStore.addCleanOutput(output);
	}

}
