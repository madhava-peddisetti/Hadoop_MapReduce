package nyc_borough;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
	

    //    hadoop datatype
    Text boroughs = new Text();
    IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] logs = line.split(",");
        
        if(logs.length>0){
        
        	String borough = logs[2];
        	
        	if (!borough.equals("BOROUGH") && !borough.isEmpty()) {
        	boroughs.set(borough);
        	}
        	context.write(boroughs,one);
        }
    }


	
	
	

}
