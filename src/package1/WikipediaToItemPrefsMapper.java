package package1;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VarLongWritable;

public class WikipediaToItemPrefsMapper extends Mapper<LongWritable,Text,VarLongWritable,VarLongWritable> {
	private static final Pattern NUMBERS = Pattern.compile("(\\d+)");
	public void map (LongWritable key,Text value,Context context) throws IOException,InterruptedException
	{
		String line =value.toString();
		Matcher m=NUMBERS.matcher(line );
		m.find();
		VarLongWritable userID=new VarLongWritable(Long.parseLong(m.group()));
		VarLongWritable itemID=new VarLongWritable();
		while(m.find())
		{
			itemID.set(Long.parseLong(m.group()));
			context.write(userID, itemID);
		}
	}

}
