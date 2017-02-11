package edu.iu.cc.tda.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.iu.cc.tda.constants.TDAConstants;

public class TemperatureDataReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		Double min = Double.MAX_VALUE, max = Double.MIN_VALUE, avg = 0.0d, sd = 0.0d;
		int counter = 0;
		List<Double> tempDataList = new ArrayList<Double>();
		for (DoubleWritable tempData_dw : values) {
			min = Math.min(min, tempData_dw.get());
			max = Math.max(max, tempData_dw.get());
			avg = avg + tempData_dw.get();
			tempDataList.add(tempData_dw.get());
			counter++;
		}
		avg = avg / counter;
		sd = this.getStandardDeviation(tempDataList, avg);

		// Context write with keys
		context.write(new Text(TDAConstants.KEY_AVG), new DoubleWritable(avg));
		context.write(new Text(TDAConstants.KEY_MAX), new DoubleWritable(max));
		context.write(new Text(TDAConstants.KEY_MIN), new DoubleWritable(min));
		context.write(new Text(TDAConstants.KEY_STD_DEV), new DoubleWritable(sd));
	}

	private Double getStandardDeviation(List<Double> values, Double meanValue) {
		Double sd = 0.0d;
		Double diffTotal = 0.0d;
		int counter = 0;
		for (Double tempData_dw : values) {
			diffTotal = diffTotal + Math.pow((tempData_dw - meanValue), 2);
			counter++;
		}
		if (counter != 0)
			sd = Math.pow((diffTotal / counter), 0.5);
		return sd;
	}

}
