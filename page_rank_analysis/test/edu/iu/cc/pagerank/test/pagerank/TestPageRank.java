package edu.iu.cc.pagerank.test.pagerank;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import edu.iu.cc.pagerank.test.io.InMemoryDataStore;
import indiana.cgl.hadoop.pagerank.PageRankMap;
import indiana.cgl.hadoop.pagerank.PageRankReduce;

public class TestPageRank {

	MapDriver<LongWritable, Text, LongWritable, Text> mapDriver;
	ReduceDriver<LongWritable, Text, LongWritable, Text> reduceDriver;
	MapReduceDriver<LongWritable, Text, LongWritable, Text, LongWritable, Text> mapReduceDriver;

	public void init() {
		PageRankMap prm = new PageRankMap();
		mapDriver = MapDriver.newMapDriver(prm);
		PageRankReduce prr = new PageRankReduce();
		reduceDriver = ReduceDriver.newReduceDriver(prr);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(prm, prr);
	}

	public void testMapReduce() throws IOException {
		this.mapReduceDriver.withAll(InMemoryDataStore.getGraphOutput()).getConfiguration().setInt("numUrls", 11);
		List<Pair<LongWritable, Text>> output = this.mapReduceDriver.run();		
		InMemoryDataStore.addPageRankOutput(output);
		this.mapReduceDriver.resetOutput();
		for (int i = 0; i < 10; i++) {
			this.mapReduceDriver.resetOutput();
			this.init();
			this.mapReduceDriver.withAll(InMemoryDataStore.getPageRankOutput()).getConfiguration().setInt("numUrls",
					11);
			output = this.mapReduceDriver.run();
			InMemoryDataStore.flushPageRankOutput();
			/*synchronized (this) {
				try {
					System.gc();
					System.out.println("Calling gc after iteration : " + i);
					TimeUnit.SECONDS.sleep(30);
				} catch (Exception ex) {
				}
			}*/
			InMemoryDataStore.addPageRankOutput(output);
		}
	}

}
