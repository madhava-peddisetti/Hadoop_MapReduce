package nyc_month_avg;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer2Class extends Reducer<Text, Text, Text, FloatWritable>  {
	

    private TreeMap<Float,String> tmap;
    
    @Override
    public void setup(Context context) throws IOException,
                                     InterruptedException
    {
        tmap = new TreeMap<Float,String>();
    }
	
	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int sum=0;
        int cnt=0;
        
        
        for(Text v: values) {
        	String nums = v.toString();
        	String num1 [] = nums.split(",");
        	sum += Integer.parseInt(num1[0]);
        	sum += Integer.parseInt(num1[1]);
          
        }
        float sum2 = (float)sum/cnt;
        
      
        
        tmap.put(sum2,key.toString());
        
       
        
        
       if (tmap.size() > 12)
        {
            tmap.remove(tmap.firstKey());
        }
	}
	 
        
        
        @Override
        
       
        public void cleanup(Context context) throws IOException,
                                           InterruptedException
                                           
        {
        	
      
            for (Map.Entry<Float, String> entry : tmap.entrySet()) 
            {
      
                float count = entry.getKey();
                String name = entry.getValue();
               
                context.write(new Text(name), new FloatWritable(count));
            }
        }
         
}
    

	


