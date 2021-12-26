package nyc_ped_compare2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
	

    //    hadoop datatype
    Text location = new Text();
    IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] logs = line.split(",");
        
        if(logs.length>0){
        
        	String loc = logs[0] + "-" + logs[1];
        	
        	
        	location.set(loc);
        	
        	context.write(location,one);
        }
    }


	
	
	

}
