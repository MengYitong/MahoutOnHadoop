package package1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.math.VectorWritable;

public class TestRun3 {
	   public static int run(String[] args) throws Exception {
	       Configuration conf = new Configuration();
	        String[] otherArgs = new GenericOptionsParser(conf, args)
	                .getRemainingArgs();
	        if (otherArgs.length != 0) {
	            System.err.println("error!");
	            System.exit(2);
	        }

	        Job job = new Job(conf, "TestRun3");
	        job.setJarByClass(TestRun3.class);
	        job.setMapperClass( CooccurrenceColumnWrapperMapper.class);
	        
	        job.setMapOutputKeyClass(IntWritable.class);
	        job.setMapOutputValueClass(VectorOrPrefWritable.class);
	        job.setOutputKeyClass(IntWritable.class);
	        job.setOutputValueClass(VectorOrPrefWritable.class);
	        job.setInputFormatClass(SequenceFileInputFormat.class);
	        job.setOutputFormatClass(SequenceFileOutputFormat.class);
	        SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.NONE);
	        FileInputFormat.addInputPath(job, new Path("hdfs://192.168.159.131:9000/output/output2/"));
	        FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.159.131:9000/output/output3/"));
	        int b=job.waitForCompletion(true) ? 0 : 1;
	        return b;
	    }
}
