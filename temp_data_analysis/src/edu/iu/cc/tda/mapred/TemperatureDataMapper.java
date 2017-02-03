package edu.iu.cc.tda.mapred;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import edu.iu.cc.tda.constants.TDAConstants;

public class TemperatureDataMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	private static final Logger LOGGER = Logger.getLogger(TemperatureDataMapper.class);

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		try {
			Double temp = new Double(value.toString());
			context.write(new Text(TDAConstants.KEY_TEMP_DATA), new DoubleWritable(temp));
		} catch (NumberFormatException nfe) {
			LOGGER.error("Invalid text data " + value.toString() + " . Ignoring and moving ahead ... ");
		}

	}

}
