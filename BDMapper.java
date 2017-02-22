import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BDMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

	private final static LongWritable one = new LongWritable(1);

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		LongWritable articleID = new LongWritable();
		Date articleDate = null, startDate = null, endDate = null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

		try {
			startDate = sdf.parse(context.getConfiguration().getStrings("startDate")[0]);
			endDate = sdf.parse(context.getConfiguration().getStrings("endDate")[0]);
		} catch (Exception e) {
		}
		String perLine[] = value.toString().split(" ");
		if (perLine[0].equals("REVISION")) {
			articleID.set(Long.parseLong(perLine[1]));
			try {
				articleDate = sdf.parse(perLine[4]);
			} catch (Exception e) {
			}
			if (articleDate.after(startDate) && articleDate.before(endDate) || articleDate.equals(startDate)
					|| articleDate.equals(endDate)) {
				context.write(articleID, one);
			}
		}
	}
}
