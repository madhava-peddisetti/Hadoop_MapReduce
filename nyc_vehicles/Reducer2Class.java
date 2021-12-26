package nyc_vehicles;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer2Class extends Reducer<Text, IntWritable, Text, Text>  {
	

    private TreeMap<Integer,String> tmap;
    long  totSum = 0;
    
    @Override
    public void setup(Context context) throws IOException,
                                     InterruptedException
    {
        tmap = new TreeMap<Integer,String>();
        
    }
	
	@Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum=0;
        for(IntWritable v: values)
            sum += v.get();
        
      
        
        tmap.put(sum,key.toString());
        
       
        
        
       if (tmap.size() > 5)
        {
            tmap.remove(tmap.firstKey());
        }
	}
	 
        
        
        @Override
        
       
        public void cleanup(Context context) throws IOException,
                                           InterruptedException
                                           
        {
        	
        	//Count Total Records 
        	

            for (Map.Entry<Integer, String> entry : tmap.entrySet()) 
            {
      
                 totSum += entry.getKey();
                
               
               
            }
        	
      
            for (Map.Entry<Integer, String> entry : tmap.entrySet()) 
            {
      
                int count = entry.getKey();
                double percentage = ((float)count/totSum)*100;
                
                DecimalFormat df = new DecimalFormat("#,###,##0.00");
                
                String per =  String.valueOf(df.format(percentage)) + " %" ;
                
                
                String name = entry.getValue();
               
                context.write(new Text(name), new Text(per));
            }
        }
         
}
    

	


