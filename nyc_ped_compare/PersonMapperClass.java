package nyc_ped_compare;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PersonMapperClass extends Mapper<LongWritable, Text, Text, Text> {
	

    //    hadoop datatype
    Text collisionId = new Text();
    Text values = new Text();
    String tempVal= " ";
    String tempVal1= " ";

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] logs = line.split(",");
        
       
        
        
  
        
        if(logs.length>0
        //	&& !logs2[1].isBlank()
        //    && !logs2[2].isBlank()
            && !logs[1].isBlank()
            && !logs[1].equals("COLLISION_ID")
        	&&  logs[5].equals("Pedestrian")
        	&&  (logs[6].equals("Injured") || logs[6].equals("Killed")))
        	
        
            
        {
        	
        		 
        		 
           
        
        	String colId = logs[1];
        	
        	tempVal = logs[5].trim()+ "," + logs[6]; 
        //	String tempVal  = logs2[1].trim() + ' ' + logs2[2].trim();
        	
        	tempVal  = "Z" + " " + tempVal ;    
        	
        	
            collisionId.set(colId);
        	values.set(tempVal);
        	
        	context.write(collisionId,values);
        }
    }
    
}


	
	
	


