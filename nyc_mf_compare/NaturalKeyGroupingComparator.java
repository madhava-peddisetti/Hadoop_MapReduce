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

public class NaturalKeyGroupingComparator extends WritableComparator 
{
	protected NaturalKeyGroupingComparator() 
	{
		super(KeyCompare.class, true);}
	
	
	public int compare(WritableComparable w1, WritableComparable w2) {
		KeyCompare k1 = (KeyCompare)w1;
		KeyCompare k2 = (KeyCompare)w2;
return k1.getMf().compareTo(k2.getMf());}}