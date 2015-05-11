package assignment2;
     
import java.io.File;
import java.io.IOException;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import assignment2.GroupingComparator;
import assignment2.SecondaryKeySortComparator;
import assignment2.TemperaturePair;



public class MapReduceProgram {
        
 
 public static class Map extends Mapper<LongWritable, Text, TemperaturePair, NullWritable> {
    
	private TemperaturePair tempPair = new TemperaturePair();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        
        String date = line.split(";")[0];
        String temperature = line.split(";")[3];
        
        tempPair.setDate(date);
        tempPair.setTemperature(temperature);
        
        context.write(tempPair, NullWritable.get());
    }
 } 
        
 public static class Reduce extends Reducer<TemperaturePair, NullWritable, TemperaturePair, NullWritable> {

    public void reduce(TemperaturePair key, Iterable<NullWritable> values, Context context) 
      throws IOException, InterruptedException {
    	
    	// We have a sorted list with temperature. Just write the first one which will be the highest
    	context.write(key, NullWritable.get());
    }
 }
 
 public static void main(String[] args) throws Exception {	 
	File s = new File("files/output/_SUCCESS");
    s.delete();
    
    File p = new File("files/output/part-r-00000");
    p.delete();
    
    Thread.sleep(1000);
    
    File f = new File("files/output");
    f.delete();
	 
    Configuration conf = new Configuration();
        
    Job job = new Job(conf, "Assignment2");
        
    job.setMapperClass(Map.class);
	job.setMapOutputKeyClass(TemperaturePair.class);
	job.setMapOutputValueClass(NullWritable.class);    
    
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    
	job.setPartitionerClass(BasicPartitioner.class);
	job.setSortComparatorClass(SecondaryKeySortComparator.class);
	job.setGroupingComparatorClass(GroupingComparator.class);
	job.setOutputKeyClass(TemperaturePair.class);
	job.setOutputValueClass(NullWritable.class);
    
	
    String inputFilePath = "files/input";
    String outputFilePath = "files/output";    
    
    FileInputFormat.addInputPath(job, new Path(inputFilePath));
    FileOutputFormat.setOutputPath(job, new Path(outputFilePath));
    
    //FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/test/output"));
    
    //job.setNumReduceTasks(4);
    
    job.waitForCompletion(true);
 }
        
}