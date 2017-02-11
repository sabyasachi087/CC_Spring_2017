package indiana.cgl.hadoop.pagerank.helper;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CleanupResultsReduce extends Reducer<PRWritable, DoubleWritable, LongWritable, DoubleWritable> {

	private static Integer COUNTER = 0;

	public void reduce(PRWritable key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		if (++COUNTER <= 10)
			context.write(key.getKey(), values.iterator().next());
	}
}