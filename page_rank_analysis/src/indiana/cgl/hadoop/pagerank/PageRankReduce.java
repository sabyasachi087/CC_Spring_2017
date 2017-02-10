package indiana.cgl.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class PageRankReduce extends Reducer<LongWritable, Text, LongWritable, Text> {

	private Logger LOGGER = Logger.getLogger(PageRankReduce.class);

	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		double sumOfRankValues = 0.0;
		StringBuilder targetUrlsList = new StringBuilder();

		int numUrls = context.getConfiguration().getInt("numUrls", 1);
		double selfRank = 0.0;
		// hints each tuple may include: rank value tuple or link relation tuple
		for (Text value : values) {
			String[] strArray = value.toString().split("#");
			if (Long.parseLong(strArray[0]) != key.get()) {
				sumOfRankValues = sumOfRankValues + Double.valueOf(strArray[1]);
				if (targetUrlsList.length() == 0)
					targetUrlsList.append(strArray[0]);
				else
					targetUrlsList.append("#" + strArray[0]);
			} else {
				selfRank = Double.valueOf(strArray[1]);
			}
		} // end for loop
		sumOfRankValues = (0.85 * sumOfRankValues) + 0.15 * ((1.0) / (double) numUrls);
		if (targetUrlsList.length() == 0) {
			LOGGER.info("Key  " + key.get() + " has no targets !!Assigning self rank ...  ");
			System.out.println("Key  " + key.get() + " has no targets !!Assigning self rank ...  ");
			sumOfRankValues = selfRank;
		}
		context.write(key, new Text(sumOfRankValues + "#" + targetUrlsList.toString()));
	}
}
