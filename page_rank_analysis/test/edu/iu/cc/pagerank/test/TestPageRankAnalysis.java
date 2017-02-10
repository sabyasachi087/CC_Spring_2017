package edu.iu.cc.pagerank.test;

import org.junit.Before;
import org.junit.Test;

import edu.iu.cc.pagerank.test.cleanup.TestCleanUp;
import edu.iu.cc.pagerank.test.graph.TestCreateGraph;
import edu.iu.cc.pagerank.test.io.InMemoryDataStore;
import edu.iu.cc.pagerank.test.pagerank.TestPageRank;

public class TestPageRankAnalysis {

	private TestCreateGraph createGraph = new TestCreateGraph();
	private TestPageRank pageRank = new TestPageRank();
	private TestCleanUp cleanUp = new TestCleanUp();

	@Before
	public void init() {
		this.createGraph.init();
		this.pageRank.init();
		this.cleanUp.init();
	}

	@Test
	public void test() {
		try {
			this.createGraph.testMapReduce();
			this.pageRank.testMapReduce();
			this.cleanUp.testMapReduce();

			InMemoryDataStore.print(InMemoryDataStore.getCleanUpOutput());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
