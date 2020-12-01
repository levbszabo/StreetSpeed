import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;
public class MergeReducer extends Reducer<Text, Text, Text, Text>{
    @Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{		
		String[] speeds = new String[8760];
        	String[] aver_speeds = new String[24];
 		for (Text value : values) {
			String[] v  = value.toString().split(",");
			if(v.length==24){
				aver_speeds= v ;
			}
			else{
				speeds = v;
			}
		}
		for(int i=0; i<speeds.length; i++){
			int j=i%24;
			if("null".equals(speeds[i])){				
				speeds[i] = aver_speeds[j];
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

