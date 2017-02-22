import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class KeyValuePair implements Writable, WritableComparable<KeyValuePair> {
	private LongWritable key;
	private LongWritable value;
	
	public KeyValuePair(){
		key = new LongWritable(0);
		value = new LongWritable(0);
	}
	
	public KeyValuePair(LongWritable key, LongWritable value){
		this.key = key;
		this.value = value;
	}
	
	public LongWritable getKey(){
		return key;
	}
	public LongWritable getValue(){
		return value;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		key.readFields(in);
		value.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		key.write(out);
		value.write(out);
	}

	@Override
	public int compareTo(KeyValuePair kvp) {
		// TODO Auto-generated method stub
		int result = value.compareTo(kvp.getValue());
		if(result == 0){
			result = this.value.compareTo(kvp.value);
		}
		return -1 * result;
	}
}
