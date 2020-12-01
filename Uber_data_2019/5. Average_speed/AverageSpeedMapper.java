import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.Arrays;

public class AverageSpeedMapper extends Mapper<LongWritable, Text, Text, Text>{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
	String line = value.toString();
	String[] s_data = line.split(",");
	String osm_start_id = s_data[9];
	String osm_end_id = s_data[10];
	String speed = s_data[11];
	String road = osm_start_id+","+osm_end_id;
	context.write(new Text(road) , new Text(speed));
	
    }
}