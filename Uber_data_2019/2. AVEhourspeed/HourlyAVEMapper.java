import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.Arrays;

public class HourlyAVEMapper extends Mapper<LongWritable, Text, Text, Text>{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
	if(key.toString().equals("0")){
	    return;
	}
	String line = value.toString();
	String[] s_data = line.split(",");
	String hour = s_data[3];
	String osm_start_id = s_data[9];
	String osm_end_id = s_data[10];
	String speed = s_data[11];
	String road_hour = null;
	if (osm_start_id!= null&&osm_end_id!=null&& hour!=null){
		road_hour = osm_start_id+","+osm_end_id+","+hour;
	}
	if(road_hour!=null&&speed!=null){
		context.write(new Text(road_hour) , new Text(speed));
	}
    }
}