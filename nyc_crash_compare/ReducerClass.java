package nyc_crash_compare;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerClass extends Reducer<KeyCompare, IntWritable, KeyCompare, IntWritable>  {
	
	Text sortKey = new Text();
	
	protected void reduce(KeyCompare key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum=0;
        for(IntWritable v: values) {
            sum += v.get();}
        
        
        
        context.write(key, new IntWritable(sum));
   }

}
