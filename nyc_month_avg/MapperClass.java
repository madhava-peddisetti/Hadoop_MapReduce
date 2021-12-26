package nyc_month_avg;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
	

    //    hadoop datatype
    Text monthSet = new Text();
    Text mainValue = new Text();
   // IntWritable one = new IntWritable(1);
    String monthString = "";
    String num1 = " ";
    String num2 = " ";
    int persons = 0;
    

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] logs = line.split(",");
        
        if(logs.length>0 && !logs[1].isBlank() && !logs[0].equals("CRASH DATE"))
        
        {
        	
        	String[] months = logs[0].split("/");
        	
        	
        	
        	int month = Integer.parseInt(months[0]);
        	int index = line.indexOf('"');
        	   
       	    if (index>0) {
       	    	   num1  = logs[11].trim();
       	    	   num2  = logs[12].trim();
       	    	   
       	    
       	                 } 
       	                  else {
       	           num1  = logs[10].trim();
       	           num2  = logs[11].trim();
       		                   }
       	    
       	           if (num1.isBlank())
       	               num1 = "0";

       	           if (num2.isBlank())
       	               num2 = "0";

       	         
       	        		   
       	 
        	
        	switch (month) {
            case 1:  monthString = "January";
                     break;
            case 2:  monthString = "February";
                     break;
            case 3:  monthString = "March";
                     break;
            case 4:  monthString = "April";
                     break;
            case 5:  monthString = "May";
                     break;
            case 6:  monthString = "June";
                     break;
            case 7:  monthString = "July";
                     break;
            case 8:  monthString = "August";
                     break;
            case 9:  monthString = "September";
                     break;
            case 10: monthString = "October";
                     break;
            case 11: monthString = "November";
                     break;
            case 12: monthString = "December";
                     break;
            default: monthString = "Invalid month";
                     break;
        }
        	
        	persons = Integer.parseInt(num1) + Integer.parseInt(num2);
        	
            monthSet.set(monthString);
            mainValue.set(String.valueOf(persons));
        	
        	context.write(monthSet,mainValue);
        
        }
    }
}
