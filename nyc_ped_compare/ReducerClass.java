package nyc_ped_compare;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerClass extends Reducer<Text, Text, Text, Text>  {
	
	private ArrayList<Text> listA = new ArrayList<Text>();
	private ArrayList<Text> listB = new ArrayList<Text>();
	
	
	
	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

      listA.clear();
      listB.clear();
      
      
      for(Text v: values) {
    	  if (v.charAt(0) == 'Y') 
    		  listA.add(new Text(v.toString().substring(1)+ ","));
    	  else if (v.charAt(0) == 'Z')
    		  listB.add(new Text(v.toString().substring(1)));
      }
      
         executeJoin(context);
          

}
	
	private void executeJoin(Context context) throws IOException, 
	InterruptedException{
		
		if(!listA.isEmpty() && !listB.isEmpty())
		{
			for(Text A: listA){
				
				for(Text B : listB)
				{
					context.write(A,B);
					}
			}
			
		}
		
}
}