/**
 * Name: Lim Bing Shun
 * GUID: 2228131L
 * 
 * Source:
 * https://vangjee.wordpress.com/2012/03/30/implementing-rawcomparator-will-speed-up-your-hadoop-mapreduce-mr-jobs-2/
 * https://vangjee.wordpress.com/2012/03/20/secondary-sorting-aka-sorting-values-in-hadoops-mapreduce-programming-paradigm/
 * WordCount.java
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BDMain extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.addResource(new Path("/local/bd4/bd4-hadoop-sit/conf/core-site.xml"));
		conf.set("mapred.jar", "file:///Documents/bdassignment.jar");
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1] + "-1"), true);
		fs.delete(new Path(args[1] + "-2"), true);
		Job job = Job.getInstance(conf, "BDAssignment_1");
		job.setJarByClass(BDMain.class);
		job.setMapperClass(BDMapper.class);
		job.setCombinerClass(BDReducer.class);
		job.setReducerClass(BDReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "-1"));
		job.getConfiguration().setStrings("startDate", args[2]);
		job.getConfiguration().setStrings("endDate", args[3]);
		job.getConfiguration().setStrings("kValue", args[4]);
		job.submit();
		job.waitForCompletion(true);
		Job job2 = Job.getInstance(conf, "BDAssignment_2");
		job2.setNumReduceTasks(1);
		job2.setJarByClass(BDMain.class);
		job2.setMapperClass(BDMapperTwo.class);
		job2.setSortComparatorClass(KVPComparator.class);
		job2.setReducerClass(BDReducerTwo.class);
		job2.setOutputKeyClass(KeyValuePair.class);
		job2.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job2, new Path(args[1] + "-1"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1] + "-2"));
		job2.getConfiguration().setStrings("kValue", args[4]);
		job2.submit();
		return job2.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 5) {
			System.out.println("java-run.sh <ClassName> <File_Path> <Start Date> <End Date> <Top K-Value>");
			System.exit(1);
		} else {
			System.exit(ToolRunner.run(new BDMain(), args));
		}
	}
}
