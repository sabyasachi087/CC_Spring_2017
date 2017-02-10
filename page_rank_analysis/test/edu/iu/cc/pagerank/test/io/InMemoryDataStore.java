package edu.iu.cc.pagerank.test.io;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.types.Pair;

public class InMemoryDataStore {

	private InMemoryDataStore() {
	}

	private static final List<Pair<LongWritable, Text>> CREATE_GRAPH_OUTPUT = new ArrayList<>();
	private static final List<Pair<LongWritable, Text>> PAGE_RANK_OUTPUT = new ArrayList<>();
	private static final List<Pair<LongWritable, Text>> CLEAN_UP_OUTPUT = new ArrayList<>();

	public static void addGraphOutput(List<Pair<LongWritable, Text>> data) {
		CREATE_GRAPH_OUTPUT.addAll(data);
	}

	public static void addPageRankOutput(List<Pair<LongWritable, Text>> data) {
		PAGE_RANK_OUTPUT.addAll(data);
	}

	public static void addCleanOutput(List<Pair<LongWritable, Text>> data) {
		CLEAN_UP_OUTPUT.addAll(data);
	}

	public static List<Pair<LongWritable, Text>> getGraphOutput() {
		return new ArrayList<Pair<LongWritable, Text>>(CREATE_GRAPH_OUTPUT);
	}

	public static List<Pair<LongWritable, Text>> getPageRankOutput() {
		return new ArrayList<Pair<LongWritable, Text>>(PAGE_RANK_OUTPUT);
	}

	public static List<Pair<LongWritable, Text>> getCleanUpOutput() {
		return new ArrayList<Pair<LongWritable, Text>>(CLEAN_UP_OUTPUT);
	}

	public static void print(List<Pair<LongWritable, Text>> datas) {
		for (Pair<LongWritable, Text> data : datas) {
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

}
