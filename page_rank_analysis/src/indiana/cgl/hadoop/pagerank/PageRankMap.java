package indiana.cgl.hadoop.pagerank;

import java.io.IOException;
import java.util.List;

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
		// instance an object that records the information for one webpage
		RankRecord rrd = new RankRecord(line);
		// int sourceUrl, targetUrl;
		// double rankValueOfSrcUrl;
		if (rrd.targetUrlsList.size() <= 0) {
			// there is no out degree for this webpage;
			// scatter its rank value to all other urls -- Why?? I can't see any
			// reason for this, commenting it out for now --TODO
			// reason for this, commenting it out for now --TODO
			/*
			 * double rankValuePerUrl = rrd.rankValue / (double) numUrls; for
			 * (int i = 0; i < numUrls; i++) { context.write(new
			 * LongWritable(i), new Text(String.valueOf(rankValuePerUrl))); }
			 */

			context.write(new LongWritable(rrd.sourceUrl),
					new Text(String.valueOf(rrd.sourceUrl + "#" + String.valueOf(rrd.rankValue / numUrls))));
			LOGGER.info("Source Url " + rrd.sourceUrl + " has no outgoing links ! ");
		} else {
			for (Long targetUrl : rrd.targetUrlsList) {
				context.write(new LongWritable(targetUrl), new Text(String
						.valueOf(rrd.sourceUrl + "#" + String.valueOf(rrd.rankValue / rrd.targetUrlsList.size()))));
			}
			context.write(new LongWritable(rrd.sourceUrl),
					new Text(String
							.valueOf(rrd.sourceUrl + "#" + String.valueOf(rrd.rankValue / rrd.targetUrlsList.size())
									+ this.reformTargetUrl(rrd.targetUrlsList))));
		}

	} // end map

	private String reformTargetUrl(List<Long> targetUrlList) {
		StringBuilder sb = new StringBuilder();
		for (Long url : targetUrlList) {
			sb.append("@" + url);
		}
		return sb.toString();
	}

}
