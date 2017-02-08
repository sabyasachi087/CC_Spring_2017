package indiana.cgl.hadoop.pagerank.helper;
		

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CreateGraphMap extends Mapper<LongWritable, Text, LongWritable, Text> {
	
	
	// creating the am matrix 
	public void map(LongWritable key, Text value, Context context)
	throws IOException, InterruptedException {

		int numUrls = context.getConfiguration().getInt("numUrls", 1);
		double val = 1.0/(double)numUrls;
		
		String[] strArray = value.toString().split(" ");
		StringBuffer sb = new StringBuffer();
		
		int sourceUrl, targetUrl;
		sourceUrl = Integer.parseInt(strArray[0]);
		sb.append(String.valueOf(val));

		for (int i=1;i<strArray.length;i++){
			targetUrl = Integer.parseInt(strArray[i]); 
			sb.append("#"+targetUrl);
		} 
		context.write(new LongWritable(sourceUrl), new Text(sb.toString()));
	}//map
}