import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;

public class WeatherReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // String[] speeds = new String[744];
        // for (Text value : values) {
        // String[] timestep_info = value.toString().split(",");
        // int day = Integer.valueOf(timestep_info[0]).intValue();
        // int hour = Integer.valueOf(timestep_info[1]).intValue();
        // // double speed = Double.valueOf(timestep_inf[2]).doubleValue();
        // String speed = timestep_info[2];
        // int index = hour + 24 * (day - 1);
        // if ((index < speeds.length) && (index >= 0)) {
        // speeds[index] = speed;
        // }
        // }
        // StringJoiner sj = new StringJoiner(",");
        // for (int i = 0; i < speeds.length; i++) {
        // sj.add(speeds[i]);
        // }
        // String outstring = sj.toString();

        StringJoiner sj = new StringJoiner(",");

        for (Text text : values) {
            sj.add(text.toString().trim());
        }

        context.write(key, new Text(sj.toString()));
    }
}