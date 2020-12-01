import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;
public class HourlyFormatReducer
    extends Reducer<Text, Text, Text, Text>{
    @Override
	public void reduce(Text key, Iterable<Text> values, Context context)
	throws IOException, InterruptedException{
        	String[] speeds = new String[24];
	for(Text value: values){
	    String[] timestep_info = value.toString().split(",");
	    int hour = Integer.valueOf(timestep_info[0]).intValue();
	    String speed = timestep_info[1];
	    if (hour < speeds.length){
	       	speeds[hour] = speed;
	    }
	}
	StringJoiner sj = new StringJoiner(",");
	for(int i = 0; i<speeds.length;i++){
	    sj.add(speeds[i]);
	}
	String outstring = sj.toString();
	context.write(key, new Text(outstring));
    }
}