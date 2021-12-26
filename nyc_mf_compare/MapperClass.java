package nyc_mf_compare;

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
    

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] logs = line.split(",");
        
        String mf = logs[12].trim();
        
        if(logs.length>23
        		&& !logs[12].isBlank()
        	    && !mf.equals("DRIVER_SEX")
        	    && !logs[23].isBlank())
        	  //  && !mf.equals("U"))
        	
                {
      
       		
        		
        		kc.setMf(mf);
        		
        		String crsDate = logs[23].trim();
        		
        		
             /*  String[] dateSplit = crsDt.split("/");
                
                try {
        		
        		 crsDate = dateSplit[2]+"/"+dateSplit[0]+"/"+dateSplit[1];
        		
                } catch (Exception e) {}; */
        		
        		kc.setConFactor(crsDate);

                
        	
        	context.write(kc,one);
        }
    }


	
	
	

}
