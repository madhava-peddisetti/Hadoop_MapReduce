package nyc_crash_compare;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<LongWritable, Text, KeyCompare, IntWritable> {
	

    //    hadoop datatype
    KeyCompare kc = new  KeyCompare();
    //private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd");
    IntWritable one = new IntWritable(1);
    String crsDate;
  

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] logs = line.split(",");
        
        if(logs.length>0){
        
        	String streetName = logs[8];
        	
   
        	
        	if (!streetName.equals("ON STREET NAME")) {
        		
        		
        		kc.setStreetName(streetName);
        		
        		String crsDt = logs[0];
        		
                String[] dateSplit = crsDt.split("/");
                
                try {
        		
        		 crsDate = dateSplit[2]+"/"+dateSplit[0]+"/"+dateSplit[1];
        		
                } catch (Exception e) {};
        		
        		kc.setCrashDate(crsDate);

                
        	}
        	context.write(kc,one);
        }
    }


	
	
	

}
