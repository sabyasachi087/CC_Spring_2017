package edu.iu.cc.pagerank.test.graph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import edu.iu.cc.pagerank.test.io.InMemoryDataStore;
import indiana.cgl.hadoop.pagerank.helper.CreateGraphMap;
import indiana.cgl.hadoop.pagerank.helper.CreateGraphReduce;

public class TestCreateGraph {

	MapDriver<LongWritable, Text, LongWritable, Text> mapDriver;
	ReduceDriver<LongWritable, Text, LongWritable, Text> reduceDriver;
	MapReduceDriver<LongWritable, Text, LongWritable, Text, LongWritable, Text> mapReduceDriver;

	public void init() {
		CreateGraphMap cgm = new CreateGraphMap();
		mapDriver = MapDriver.newMapDriver(cgm);
		CreateGraphReduce cgr = new CreateGraphReduce();
		reduceDriver = ReduceDriver.newReduceDriver(cgr);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(cgm, cgr);
	}

	public void testMapReduce() throws IOException {
		this.mapReduceDriver.withAll(getInputs()).getConfiguration().setInt("numUrls", 11);
		List<Pair<LongWritable, Text>> output = this.mapReduceDriver.run();
		InMemoryDataStore.addGraphOutput(output);
	}

	private List<Pair<LongWritable, Text>> getInputs() {
		List<Pair<LongWritable, Text>> inputs = new ArrayList<>();
		inputs.add(new Pair<LongWritable, Text>(new LongWritable(), new Text("0")));
		inputs.add(new Pair<LongWritable, Text>(new LongWritable(), new Text("1 2")));
		inputs.add(new Pair<LongWritable, Text>(new LongWritable(), new Text("2 1")));
		inputs.add(new Pair<LongWritable, Text>(new LongWritable(), new Text("3 0 1")));
		inputs.add(new Pair<LongWritable, Text>(new LongWritable(), new Text("4 1 3 5")));
		inputs.add(new Pair<LongWritable, Text>(new LongWritable(), new Text("5 1 4")));
		inputs.add(new Pair<LongWritable, Text>(new LongWritable(), new Text("6 1 4")));
		inputs.add(new Pair<LongWritable, Text>(new LongWritable(), new Text("7 1 4")));
		inputs.add(new Pair<LongWritable, Text>(new LongWritable(), new Text("8 1 4")));
		inputs.add(new Pair<LongWritable, Text>(new LongWritable(), new Text("9 4")));
		inputs.add(new Pair<LongWritable, Text>(new LongWritable(), new Text("10 4")));
		return inputs;
	}

	@SuppressWarnings("unused")
	private void print(List<Pair<LongWritable, Text>> datas) {
		for (Pair<LongWritable, Text> data : datas) {
			System.out.println("Key -> " + data.getFirst() + " , Value -> " + data.getSecond());
		}
	}

}
