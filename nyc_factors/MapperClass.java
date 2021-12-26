package nyc_factors;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
	

    //    hadoop datatype
    Text vehicleType = new Text();
    IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] logs = line.split(",");
        
        if(logs.length>0){
        
        	String veh_type = logs[6];
        	
        	if (!veh_type.equals("VEHICLE_TYPE")) {
        	vehicleType.set(veh_type);
        	}
        	context.write(vehicleType,one);
        }
    }


	
	
	

}
