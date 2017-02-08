package indiana.cgl.hadoop.pagerank;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMap extends Mapper<LongWritable, Text, LongWritable, Text> {

	private static final Log LOGGER = LogFactory.getLog(PageRankMap.class);

	// each map task handles one line within an adjacency matrix file
	// key: file offset
	// value: <sourceUrl PageRank#targetUrls>
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		int numUrls = context.getConfiguration().getInt("numUrls", 1);
		String line = value.toString();
		LOGGER.info("Text value for " + key + " >> " + line);
		StringBuffer sb = new StringBuffer();
		// instance an object that records the information for one webpage
		RankRecord rrd = new RankRecord(line);
		// int sourceUrl, targetUrl;
		// double rankValueOfSrcUrl;
		if (rrd.targetUrlsList.size() <= 0) {
			// there is no out degree for this webpage;
			// scatter its rank value to all other urls
			double rankValuePerUrl = rrd.rankValue / (double) numUrls;
			for (int i = 0; i < numUrls; i++) {
				context.write(new LongWritable(i), new Text(String.valueOf(rankValuePerUrl)));
			}
		} else {
			/* Write your code here */
		} // for
		context.write(new LongWritable(rrd.sourceUrl), new Text(sb.toString()));
	} // end map

}
