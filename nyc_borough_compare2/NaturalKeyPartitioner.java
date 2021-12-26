package nyc_borough_compare2;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;

public class NaturalKeyPartitioner extends Partitioner<KeyCompare, DoubleWritable>
{
@Override
public int getPartition(KeyCompare key, DoubleWritable val, int numPartitions) {
int hash = key.getStreetName().hashCode();
int partition = hash % numPartitions;
return partition;
}
}