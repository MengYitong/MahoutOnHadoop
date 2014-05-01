package package1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.math.VarIntWritable;

public class ToVectorAndPrefMapper extends Mapper<IntWritable,VectorOrPrefWritable,VarIntWritable,VectorOrPrefWritable>
{
    public void map(IntWritable key, VectorOrPrefWritable value, Context context) throws IOException, InterruptedException 
    {
    	int Key=key.get();
    	context.write(new VarIntWritable(Key), value);
    }
	
}
