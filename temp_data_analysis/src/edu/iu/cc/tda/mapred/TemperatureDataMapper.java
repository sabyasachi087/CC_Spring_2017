package edu.iu.cc.tda.mapred;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.iu.cc.tda.constants.TDAConstants;

public class TemperatureDataMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		try {
			Double temp = new Double(value.toString());
			context.write(new Text(TDAConstants.KEY_TEMP_DATA), new DoubleWritable(temp));
		} catch (NumberFormatException nfe) {
			System.err.println("Invalid text data " + value.toString() + " . Ignoring and moving ahead ... ");
		}

	}

}
