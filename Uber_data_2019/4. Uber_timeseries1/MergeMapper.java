import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.Arrays;

public class MergeMapper extends Mapper<LongWritable, Text, Text, Text>{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
	String line = value.toString();
	String[] d = line.split("\t");
	String road = d[0];
	String speeds = d[1];
 	context.write(new Text(road) , new Text(speeds));
    }
}