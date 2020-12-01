import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class Merge2 {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
		System.err.println("Usage: Merge2 <input path> <output path>");
		System.exit(-1);
		}
		Job job = new Job();
		job.setJarByClass(Merge2.class);
		job.setJobName("Merge2");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));	
		job.setMapperClass(Merge2Mapper.class);
		job.setReducerClass(Merge2Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1); 
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}


