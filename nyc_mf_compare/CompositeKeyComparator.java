package nyc_mf_compare;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;

public class CompositeKeyComparator extends WritableComparator 
{
	protected CompositeKeyComparator() 
	{
		super(KeyCompare.class, true);}
	
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		KeyCompare k1 = (KeyCompare)w1;
		KeyCompare k2 = (KeyCompare)w2;
		int result = 0;
		try {
		 result = k1.getMf().compareTo(k2.getMf());
		if(result == 0) {
         result = -1*k1.getConFactor().compareTo(k2.getConFactor());

}
		}
		catch (Exception e) {
		      
		    }
		return result;
	}
}