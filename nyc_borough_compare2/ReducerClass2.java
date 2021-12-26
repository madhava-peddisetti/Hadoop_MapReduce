package nyc_borough_compare2;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ReducerClass2 extends Reducer<KeyCompare, IntWritable, Text, IntWritable>  {
	
	Text sortKey = new Text();
	 private TreeMap<String,Integer> tmap;
	 
	 private TreeMap<Integer,String> tmapTemp;
	 
	 @Override
	    public void setup(Context context) throws IOException,
	                                     InterruptedException
	    {
	        tmap = new TreeMap<String,Integer>();
	        tmapTemp = new TreeMap<Integer,String>();
	        
	    }
		
	
	protected void reduce(KeyCompare key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum=0;
        String temp = " ";
        
        for(IntWritable v: values) {
        	if (temp.equals(" "))
        		temp = key.getStreetName();
        	
        	
        	if (!temp.equals(key.getStreetName())) {
        	temp = key.getStreetName();
        	tmapTemp.put(sum,key.toString());
        	
        	// Add Top 5 
        	 if (tmapTemp.size() > 10)
             {
                 tmapTemp.remove(tmapTemp.firstKey());
             }
        	sum = 0;}
        	
            sum += v.get();
        }
        
        
       //Add from Temp tmapTemp to Main tmap
        
        

        for (Map.Entry<Integer,String> entry : tmapTemp.entrySet()) 
        {
  
            int count = entry.getKey();
            String name = entry.getValue();
           
            tmap.put(name,count);
            ;
        }
        
        tmapTemp.clear();
    }
        
        
        

       
        
       // context.write(key, new IntWritable(sum));
   
	
	 @Override
     
     
     public void cleanup(Context context) throws IOException,
                                        InterruptedException
                                        
     {
     	
   
         for (Map.Entry<String,Integer> entry : tmap.entrySet()) 
         {
   
             int count = entry.getValue();
             String name = entry.getKey();
            
             context.write(new Text(name), new IntWritable(count));
         }
     }

}
