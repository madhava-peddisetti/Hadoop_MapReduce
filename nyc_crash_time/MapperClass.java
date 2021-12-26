package nyc_crash_time;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
	

    //    hadoop datatype
    Text timeOfDay = new Text();
    IntWritable one = new IntWritable(1);
    String timeDay  = " ";

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] logs = line.split(",");
        
        if(logs.length>0 && !logs[1].isBlank() && !logs[1].equals("CRASH TIME"))
        
        {
        	
        	String[] time = logs[1].split(":");
        	
        	int hr = Integer.parseInt(time[0]);
        
        	if (hr > 3 && hr < 12) {
        		
        		timeDay = "Morning";
        		
        	}
        	

       

        	if (hr > 11 && hr < 5) {
        		
        		timeDay = "Afternoon";
        		
        	}
        	

        	if (hr > 4 && hr < 7) {
        		
        		timeDay = "Evening";
        		
        	}
        	
            if (hr > 6 && hr < 4) {
        		
        		timeDay = "Night";
        		
        	}
        	
        	
        	if (!timeDay.isBlank()){
        	
        	timeOfDay.set(timeDay);
        	
        	context.write(timeOfDay,one);
        	}
        }
    }


	
	
	

}
