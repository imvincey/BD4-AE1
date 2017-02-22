import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BDMapperTwo extends Mapper<LongWritable, Text, KeyValuePair, LongWritable> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] resultSplit = value.toString().split("\t");
		LongWritable KVPkey = new LongWritable();
		KVPkey.set(Long.parseLong(resultSplit[0]));
		LongWritable KVPvalue = new LongWritable();
		KVPvalue.set(Long.parseLong(resultSplit[1]));
		KeyValuePair kvp = new KeyValuePair(KVPkey, KVPvalue);
		context.write(kvp, new LongWritable(1));
	}
}
