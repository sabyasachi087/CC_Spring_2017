package iu.pti.hbaseapp.clueweb09;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import iu.pti.hbaseapp.Constants;

public class FreqIndexBuilderClueWeb09 {
	/**
	 * Internal Mapper to be run by Hadoop.
	 */
	public static class FibMapper extends TableMapper<ImmutableBytesWritable, Put> {
		@Override
		protected void map(ImmutableBytesWritable rowKey, Result result, Context context)
				throws IOException, InterruptedException {
			byte[] docIdBytes = rowKey.get();
			byte[] contentBytes = result.getValue(Constants.CF_DETAILS_BYTES, Constants.QUAL_CONTENT_BYTES);
			String content = Bytes.toString(contentBytes);
			Map<String, Integer> data = getTermFreqs(content);

			Put column;
			Cell cell;
			for (Entry<String, Integer> entry : data.entrySet()) {
				column = new Put(entry.getKey().getBytes());
				/*cellDocId = CellUtil.createCell(entry.getKey().getBytes(), Constants.CF_FREQUENCIES_BYTES,
						"documentId".getBytes(), System.currentTimeMillis(), KeyValue.Type.Put.getCode(), docIdBytes);
				cellFrequency = CellUtil.createCell(entry.getKey().getBytes(), Constants.CF_FREQUENCIES_BYTES,
						"value".getBytes(), System.currentTimeMillis(), KeyValue.Type.Put.getCode(),
						Bytes.toBytes(entry.getValue()));
				column.add(cellDocId);
				column.add(cellFrequency);*/
				cell = CellUtil.createCell(entry.getKey().getBytes(), Constants.CF_FREQUENCIES_BYTES,
						docIdBytes, System.currentTimeMillis(), KeyValue.Type.Put.getCode(), Bytes.toBytes(entry.getValue()));
				column.add(cell);
				context.write(new ImmutableBytesWritable(entry.getKey().getBytes()), column);
			}

		}
	}

	/**
	 * get the terms, their frequencies and positions in a given string using a
	 * Lucene analyzer
	 * 
	 * @param text
	 * 
	 */
	public static HashMap<String, Integer> getTermFreqs(String text) {
		HashMap<String, Integer> freqs = new HashMap<String, Integer>();
		try {
			Analyzer analyzer = Constants.analyzer;
			TokenStream ts = analyzer.tokenStream("dummyField", new StringReader(text));
			CharTermAttribute charTermAttr = ts.addAttribute(CharTermAttribute.class);
			ts.reset();
			while (ts.incrementToken()) {
				String termVal = charTermAttr.toString();
				if (Helpers.isNumberString(termVal)) {
					continue;
				}

				if (freqs.containsKey(termVal)) {
					freqs.put(termVal, freqs.get(termVal) + 1);
				} else {
					freqs.put(termVal, 1);
				}
			}
			ts.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return freqs;
	}

	/**
	 * Job configuration.
	 */
	public static Job configureJob(Configuration conf, String[] args) throws IOException {
		conf.set("mapreduce.map.speculative", "false");
		conf.set("mapreduce.reduce.speculative", "false");
		Scan scan = new Scan();
		scan.addColumn(Constants.CF_DETAILS_BYTES, Constants.QUAL_CONTENT_BYTES);
		Job job = Job.getInstance(conf);
		// Job job = new Job(conf, "Building freq_index from " +
		// Constants.CLUEWEB09_DATA_TABLE_NAME);
		job.setJobName("Building freq_index from " + Constants.CLUEWEB09_DATA_TABLE_NAME);
		job.setJarByClass(FibMapper.class);
		TableMapReduceUtil.initTableMapperJob(Constants.CLUEWEB09_DATA_TABLE_NAME, scan, FibMapper.class,
				ImmutableBytesWritable.class, Put.class, job, true);
		TableMapReduceUtil.initTableReducerJob(Constants.CLUEWEB09_INDEX_TABLE_NAME, null, job);
		job.setNumReduceTasks(0);

		return job;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = configureJob(conf, otherArgs);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
