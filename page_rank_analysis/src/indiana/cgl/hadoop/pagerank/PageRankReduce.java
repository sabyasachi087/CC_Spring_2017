package indiana.cgl.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReduce extends Reducer<LongWritable, Text, LongWritable, Text> {
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		double sumOfRankValues = 0.0;
		StringBuilder targetUrlsList = new StringBuilder();

		int numUrls = context.getConfiguration().getInt("numUrls", 1);
		// hints each tuple may include: rank value tuple or link relation tuple
		for (Text value : values) {
			String[] strArray = value.toString().split("#");
			sumOfRankValues = sumOfRankValues + Double.valueOf(strArray[1]);
			if (targetUrlsList.length() == 0)
				targetUrlsList.append(strArray[0]);
			else
				targetUrlsList.append("#" + strArray[0]);
		} // end for loop
		sumOfRankValues = 0.85 * sumOfRankValues + 0.15 * (1.0) / (double) numUrls;
		context.write(key, new Text(sumOfRankValues + "#" + targetUrlsList.toString()));
	}
}
