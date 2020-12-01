import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;
public class UberReducer
    extends Reducer<Text, Text, Text, Text>{
    @Override
	public void reduce(Text key, Iterable<Text> values, Context context)
	throws IOException, InterruptedException{
	int[] monthly_hour = {0,744,1416,2160,2880,3624,4344,5088,5832,6552,7296,8016};
        String[] speeds = new String[8760];
	for(Text value: values){
	    String[] timestep_info = value.toString().split(",");
	    int month =Integer.valueOf(timestep_info[0]).intValue() ;
	    int day = Integer.valueOf(timestep_info[1]).intValue();
	    int hour = Integer.valueOf(timestep_info[2]).intValue();
	    String speed = timestep_info[3];
	    int index = monthly_hour[(month-1)] + hour + 24*(day-1);
	    if ((index < speeds.length) && (index >= 0)){
	       	speeds[index] = speed;
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