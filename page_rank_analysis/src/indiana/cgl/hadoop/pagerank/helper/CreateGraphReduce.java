package indiana.cgl.hadoop.pagerank.helper;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CreateGraphReduce extends Reducer<LongWritable, Text, LongWritable, Text> {

	private static final Log LOGGER = LogFactory.getLog(CreateGraphReduce.class);

	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException {

		try {

			Text outputValue = values.iterator().next();
			context.write(key, outputValue);
		} catch (InterruptedException e) {
			e.printStackTrace();
			LOGGER.error(e);
		}
	}
}