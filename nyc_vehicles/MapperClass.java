package nyc_vehicles;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
	

    //    hadoop datatype
    Text vehicle = new Text();
    IntWritable one = new IntWritable(1);
    String vehicleMake = " ";

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] logs = line.split(",");
        
        
        
        if(logs.length>0){
        
        
        	
        	if (!logs[7].equals("VEHICLE_TYPE") && !logs[7].isEmpty()) {
        		//String vehicleMake = logs[7].substring(0,4);
        		
        		
        		String [] vehMk = logs[7].split("-");
        		
        		if (vehMk[0].equals("CHEV ")) {
            		
            		vehicleMake = "CHEVROLET";
            		
            	}

        		if (vehMk[0].equals("FORD ")) {
            		
            		vehicleMake = "FORD";
            		
        		}
                
               if (vehMk[0].equals("NISS ")) {
            		
            		vehicleMake = "NISSAN";
            		
        		}
               if (vehMk[0].equals("HOND ")) {
           		
           		vehicleMake = "HONDA";
           		
       		   }
               
               if (vehMk[0].equals("TOYT ")) {
              		
              		vehicleMake = "TOYOTA";
              		
          		   }
           

            	
            	
        		
            	vehicle.set(vehicleMake);
            	}
            	context.write(vehicle,one);
            }
    }


	
	
	

}
