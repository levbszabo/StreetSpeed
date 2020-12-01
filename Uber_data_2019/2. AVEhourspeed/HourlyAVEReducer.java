import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;
public class HourlyAVEReducer extends Reducer<Text, Text, Text, Text>{
    @Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{		
		double count = 0;
		double sum = 0;

 		for (Text value : values) {
			String v = value.toString();
			double val =Double.valueOf(v).doubleValue();
			sum = sum + val;
			count += 1;
		}
		double aver = sum/count;
		String ave = String.format("%.2f", aver);
		context.write(key, new Text(ave));
	}
}

