import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.Arrays;

public class UberMapper 
    extends Mapper<LongWritable, Text, Text, Text>{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
	if(key.toString().equals("0")){
	    return;
	}
	String line = value.toString();
	String[] inputArray = line.split(",");
	String month = inputArray[1];
                 String day = inputArray[2];
	String hour = inputArray[3];
	String osm_start_id = inputArray[9];
	String osm_end_id = inputArray[10];
	String avg_speed = inputArray[11];
	
	String intermediate_key = osm_start_id + "," + osm_end_id;
	String timestep_speed =  month+","+day + "," + hour + "," + avg_speed;
	context.write(new Text(intermediate_key) , new Text(timestep_speed));
    }
}