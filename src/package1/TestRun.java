package package1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.VectorWritable;


public class TestRun {
    public static int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 0) {
            System.err.println("error!");
            System.exit(2);
        }
        Job job = new Job(conf, "word count");
        job.setJarByClass(TestRun.class);
        job.setMapperClass(WikipediaToItemPrefsMapper.class);
        job.setReducerClass(WikipediaToUserVectorReducer.class);
        job.setMapOutputKeyClass(VarLongWritable.class);
        job.setMapOutputValueClass(VarLongWritable.class);
        job.setOutputKeyClass(VarLongWritable.class);
        job.setOutputValueClass(VectorWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.NONE);
        FileInputFormat.addInputPath(job, new Path("hdfs://192.168.159.131:9000/input/"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.159.131:9000/output/output1/"));
        int b=job.waitForCompletion(true) ? 0 : 1;
        return b;
    }

}
