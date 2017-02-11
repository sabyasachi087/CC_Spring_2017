package indiana.cgl.hadoop.pagerank.helper;

import java.io.IOException;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * collect the page rank results from previous computation.
 */

import indiana.cgl.hadoop.pagerank.RankRecord;

public class CleanupResultsMap extends Mapper<LongWritable, Text, PRWritable, DoubleWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String strLine = value.toString();
		RankRecord rrd = new RankRecord(strLine);
		PRWritable prKey = new PRWritable(new LongWritable(rrd.sourceUrl), new DoubleWritable(rrd.rankValue));
		context.write(prKey, new DoubleWritable(rrd.rankValue));
	}

}
