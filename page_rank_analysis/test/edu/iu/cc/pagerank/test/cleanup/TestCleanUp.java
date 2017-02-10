package edu.iu.cc.pagerank.test.cleanup;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import edu.iu.cc.pagerank.test.io.InMemoryDataStore;
import indiana.cgl.hadoop.pagerank.helper.CleanupResultsMap;
import indiana.cgl.hadoop.pagerank.helper.CleanupResultsReduce;

public class TestCleanUp {

	MapDriver<LongWritable, Text, LongWritable, Text> mapDriver;
	ReduceDriver<LongWritable, Text, LongWritable, Text> reduceDriver;
	MapReduceDriver<LongWritable, Text, LongWritable, Text, LongWritable, Text> mapReduceDriver;

	public void init() {
		CleanupResultsMap crm = new CleanupResultsMap();
		mapDriver = MapDriver.newMapDriver(crm);
		CleanupResultsReduce crr = new CleanupResultsReduce();
		reduceDriver = ReduceDriver.newReduceDriver(crr);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(crm, crr);
	}

	public void testMapReduce() throws IOException {
		this.mapReduceDriver.withAll(InMemoryDataStore.getPageRankOutput()).getConfiguration().setInt("numUrls", 11);
		List<Pair<LongWritable, Text>> output = this.mapReduceDriver.run();
		InMemoryDataStore.addCleanOutput(output);
	}

}
