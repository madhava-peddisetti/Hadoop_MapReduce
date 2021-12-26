package nyc_ped_compare;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CrashMapperClass extends Mapper<LongWritable, Text, Text, Text> {
	

    //    hadoop datatype
    Text collisionId = new Text();
    Text values = new Text();
    String tempVal = " ";
    String colId = " ";

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] logs = line.split(",");
        
        int index = line.indexOf('"');
    	   
   	    if (index>0) {
   	    	   tempVal  = logs[2].trim() + "," + logs[8].trim();
    		    colId = logs[24]; 
   	 }else {
   		if(logs.length>23 && !logs[1].equals("CRASH DATE")){
   		   tempVal  = logs[2].trim() + ","+ logs[7].trim();
   		colId = logs[23];
   		}
   	 }
   		 
        
        if(logs.length>23
        	&& !logs[2].isBlank()
          //  && !colId.isBlank()
            && !logs[2].equals("BOROUGH"))
           // &&  colId.equals("4230968"))
        	
            
        {
        
    
            collisionId.set(colId);
            tempVal  = "Y" + " " + tempVal ;
        	values.set(tempVal);
        	
        	context.write(collisionId,values);
        }
        
    }
}

	
	
	


