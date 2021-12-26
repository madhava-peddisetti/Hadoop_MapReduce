package nyc_bloom;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

public class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
	

    //    hadoop datatype
    Text vehicleSelect = new Text();
    IntWritable one = new IntWritable(1);
    DataInputStream dataInputStream;

    
    
    private BloomFilter filter = new BloomFilter(40,10,Hash.MURMUR_HASH);
    
    static String test = "Sedan";
    
    static Key key1 = new Key(test.getBytes());
	
    
    protected void setup(Context context) throws IOException, InterruptedException, EOFException{
    	
    	// Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration().get("bloom_filter_file_location"));
    	
    	/*try {
    	dataInputStream = new DataInputStream(
    	new FileInputStream(context.getConfiguration().get("bloom_filter_file_location")));
    	filter.readFields(dataInputStream);
    	//dataInputStream.close();
    	}catch (EOFException e) {
    		dataInputStream.close();
    	}*/
    	}
    	
    	

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] logs = line.split(",");
        
      
    	filter.add(key1);
       
        
        if(logs.length>0){
        
        	String vehicle = logs[6].trim();
       
        	
        	
        	if (!vehicle.equals("VEHICLE_TYPE") && !vehicle.isEmpty()) {
        		
        		if (filter.membershipTest(new Key(vehicle.trim().getBytes())))
        		{ 
        			vehicleSelect.set(vehicle);
        	
        
      
        			
        	context.write(vehicleSelect,one);}
        }
    }

    }
	

}
