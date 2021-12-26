package nyc_borough_compare2;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;

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
    static final List<String> boroughs = Arrays.asList("BROOKLYN", "QUEENS", "BRONX");
    

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] logs = line.split(",");
        
        if(logs.length>0 && boroughs.contains(logs[2]) 
        		&& !logs[8].isBlank()) {
        
        	String borough = logs[2].trim();
        	
   
        	
        	if (!borough.equals("BOROUGH") && !borough.isEmpty())  {
        		
        		
        		kc.setBorough(borough);
        		
        		String crsDate = logs[8].trim();
        		
             /*  String[] dateSplit = crsDt.split("/");
                
                try {
        		
        		 crsDate = dateSplit[2]+"/"+dateSplit[0]+"/"+dateSplit[1];
        		
                } catch (Exception e) {}; */
        		
        		kc.setStreetName(crsDate);

                
        	}
        	context.write(kc,one);
        }
    }


	
	
	

}
