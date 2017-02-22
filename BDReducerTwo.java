import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BDReducerTwo extends Reducer<KeyValuePair, LongWritable, Text, Text> {
	static int count = 0;
	
	public void reduce(KeyValuePair key, Iterable<LongWritable> value, Context context) throws IOException, InterruptedException {
		int kvalue = Integer.parseInt(context.getConfiguration().getStrings("kValue")[0]);
		count ++;
		Text kvpkey = new Text();
		Text kvpvalue = new Text();

		for (LongWritable kvp : value){
			kvpkey.set(key.getKey().toString());
			kvpvalue.set(key.getValue().toString());
			if (count <= kvalue){
				context.write(kvpkey, kvpvalue);
			}
		}
	}
}
