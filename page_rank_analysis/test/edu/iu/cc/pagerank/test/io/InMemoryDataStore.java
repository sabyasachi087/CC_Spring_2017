package edu.iu.cc.pagerank.test.io;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.types.Pair;

public class InMemoryDataStore {

	private InMemoryDataStore() {
	}

	private static final List<Pair<LongWritable, Text>> CREATE_GRAPH_OUTPUT = new ArrayList<Pair<LongWritable, Text>>();
	private static final List<Pair<LongWritable, Text>> PAGE_RANK_OUTPUT = new ArrayList<Pair<LongWritable, Text>>();
	private static final List<Pair<LongWritable, DoubleWritable>> CLEAN_UP_OUTPUT = new ArrayList<Pair<LongWritable, DoubleWritable>>();

	public static void addGraphOutput(List<Pair<LongWritable, Text>> datas) {
		for (Pair<LongWritable, Text> data : datas) {
			CREATE_GRAPH_OUTPUT.add(new Pair<LongWritable, Text>(new LongWritable(getRandomValue()),
					new Text(data.getFirst() + "\t" + data.getSecond())));
		}
	}

	public static void addPageRankOutput(List<Pair<LongWritable, Text>> datas) {
		for (Pair<LongWritable, Text> data : datas) {
			PAGE_RANK_OUTPUT.add(new Pair<LongWritable, Text>(new LongWritable(getRandomValue()),
					new Text(data.getFirst() + "\t" + data.getSecond())));
		}
	}

	public static void addCleanOutput(List<Pair<LongWritable, DoubleWritable>> datas) {
		/*for (Pair<LongWritable, Text> data : datas) {
			CLEAN_UP_OUTPUT.add(new Pair<LongWritable, Text>(new LongWritable(getRandomValue()),
					new Text(data.getFirst() + "\t" + data.getSecond())));
		}*/
		CLEAN_UP_OUTPUT.addAll(datas);
	}

	public static List<Pair<LongWritable, Text>> getGraphOutput() {
		return new ArrayList<Pair<LongWritable, Text>>(CREATE_GRAPH_OUTPUT);
	}

	public static List<Pair<LongWritable, Text>> getPageRankOutput() {
		return new ArrayList<Pair<LongWritable, Text>>(PAGE_RANK_OUTPUT);
	}

	public static List<Pair<LongWritable, DoubleWritable>> getCleanUpOutput() {
		return new ArrayList<Pair<LongWritable, DoubleWritable>>(CLEAN_UP_OUTPUT);
	}

	public static void print() {
		for (Pair<LongWritable, DoubleWritable> data : CLEAN_UP_OUTPUT) {
			System.out.println("Key -> " + data.getFirst() + " , Value -> " + data.getSecond());
		}
	}

	public static void flushGraphOutput() {
		CREATE_GRAPH_OUTPUT.clear();
	}

	public static void flushPageRankOutput() {
		PAGE_RANK_OUTPUT.clear();
	}

	public static void flushCleanUpOutput() {
		CLEAN_UP_OUTPUT.clear();
	}
	
	private static Long getRandomValue(){
		return (long)(Math.random()*1000+1);
	}

}
