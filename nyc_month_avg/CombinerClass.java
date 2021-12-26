package nyc_month_avg;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CombinerClass extends Reducer<Text, Text, Text, Text>  {
	

  Text mainValue = new Text();
  String mainVal = " ";
   
	
	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int sum=0;
        int cnt=0;
        String num = " ";
        
        for(Text v: values) {
            num = v.toString();
            sum += Integer.parseInt(num);
            cnt += 1;}
        
         mainVal  = String.valueOf(sum) + "," + String.valueOf(cnt);
        
        
        
        context.write(key,mainValue);
        
        
   
        
      
    
        
      
	}
	 
        
        
       
}
    

	


