import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.Arrays;
import java.util.StringJoiner;

public class WeatherMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.toString().equals("0")) {
            return;
        }
        String line = value.toString().replace("\"", "");
        String[] inputArray = line.split(",", -1);
        // "72505394728","2020-03-01T00:51:00","FM-15","7",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,"30.10","8","25","0.00",,"-0.04","3","48","30.07","CLR:00","29.93","10.00","20","VRB","20","11",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,"MET12403/01/20
        // 00:51:02 METAR KNYC 010551Z 30010G17KT 10SM CLR M04/M13 A3010 RMK AO2 SLP184
        // T10391133 11017 21039 53014 $ RTX
        // (JE)","FM-15","7",,,,,,,,,,,,,,,,,,,,,,,,,,,,

        // 'date': 1,
        // 'HourlyDryBulbTemperature': 43,
        // 'HourlyPrecipitation': 44,
        // 'HourlyPresentWeatherType': 45,
        // 'HourlySkyConditions': 50,
        // 'HourlyVisibility': 52,
        // 'HourlyWindGustSpeed': 55,
        // 'HourlyWindSpeed': 56,
        // 'REM': 93
        String date = inputArray[1];
        String REPORT_TYPE = inputArray[2];

        // SOD = summary of day, SOM = summary of month
        if (!date.startsWith("2019") | (REPORT_TYPE.startsWith("SO"))) {
            return;
        }

        // time series format: 2020-01-01 00:00:00
        String timeSeries = date.substring(0, 14).replace("T", " ") + "00:00";

        String HourlyDryBulbTemperature = inputArray[43];
        String HourlyPrecipitation = inputArray[44];
        String HourlyPresentWeatherType = inputArray[45];
        String HourlySkyConditions = inputArray[50];
        String HourlyVisibility = inputArray[52];
        String HourlyWindGustSpeed = inputArray[55];
        String HourlyWindSpeed = inputArray[56];
        String REM = inputArray[93];

        StringJoiner data = new StringJoiner(",");
        data.add(HourlyDryBulbTemperature);
        data.add(HourlyPrecipitation);
        data.add(HourlyPresentWeatherType);
        data.add(HourlySkyConditions);
        data.add(HourlyVisibility);
        data.add(HourlyWindGustSpeed);
        data.add(HourlyWindSpeed);

        context.write(new Text(timeSeries), new Text(data.toString()));
    }
}