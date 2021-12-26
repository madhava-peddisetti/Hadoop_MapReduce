package nyc_crash_compare2;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd");
	
	
    //    hadoop datatype
    Text street = new Text();
    IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] logs = line.split(",");
        
        if(logs.length>0){
        
        	//String streetName  = logs[8];
        	
        	String crsDate = logs[0];
        	
        	if (!crsDate.equals("CRASH DATE")) {
        		
        		String[] dateSplit = crsDate.split("/");
        		
        		street.set(dateSplit[2]+"/"+dateSplit[0]+"/"+dateSplit[1]);
        	} 
        	
		/*	try {
				crsDate = frmt.parse(logs[0]).toString();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	
        	if (!crsDate.equals("CRASH DATE")) {
        		street.set(crsDate);
        	} */
        	context.write(street,one);
        }
    }


	
	
	

}
