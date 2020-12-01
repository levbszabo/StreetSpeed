import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.Arrays;

public class HourlyFormatMapper extends Mapper<LongWritable, Text, Text, Text>{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
	String line = value.toString().trim();
	String[] d1 = line.split("\t");
	String speed = d1[1];
	String[] d2 = d1[0].split(",");
	String start = d2[0];
	String end = d2[1];
	String hour = d2[2];
	String k = start + "," + end;
	String v = hour + "," + speed;
 	context.write(new Text(k) , new Text(v));
    }
}