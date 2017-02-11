package indiana.cgl.hadoop.pagerank.helper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class PRWritable implements WritableComparable<PRWritable> {

	private LongWritable key;
	private DoubleWritable value;

	public PRWritable(LongWritable key, DoubleWritable value) {
		super();
		this.key = key;
		this.value = value;
	}

	public PRWritable() {
		super();
		this.key = new LongWritable();
		this.value = new DoubleWritable();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.key.write(out);
		this.value.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.key.readFields(in);
		this.value.readFields(in);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof PRWritable))
			return false;
		PRWritable other = (PRWritable) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		return true;
	}

	@Override
	public int compareTo(PRWritable o) {
		int result = this.value.compareTo(o.value);
		if (result == 0) {
			result = this.key.compareTo(o.key);
		}
		return result * -1;
	}

	public LongWritable getKey() {
		return key;
	}

	public DoubleWritable getValue() {
		return value;
	}

}
