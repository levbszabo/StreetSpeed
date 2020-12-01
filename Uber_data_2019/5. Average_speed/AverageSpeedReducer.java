import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;
public class AverageSpeedReducer extends Reducer<Text, Text, Text, Text>{
    @Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{		
		double count = 0;
		double s_sum = 0;
 		for (Text value : values) {
			double speed =Double.valueOf(value.toString()).doubleValue();
			s_sum += speed;
			count += 1;
		}
		double aver = s_sum/count;
		String ave = String.format("%.2f", aver);
		context.write(key, new Text(ave));
	}
}

