

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Join{
 
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
		      System.err.println("Usage: Join <input path> <output path>");
		      System.exit(-1);
		    }

			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "join");
			job.setJarByClass(Join.class);    
		

		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));

		    job.setMapperClass(JoinMapper.class);
		    job.setReducerClass(JoinReducer.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(TuplePair.class);

		    job.setOutputKeyClass(NullWritable.class);
		    job.setOutputValueClass(Text.class);

		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}
