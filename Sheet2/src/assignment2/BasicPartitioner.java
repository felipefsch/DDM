package assignment2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * This sends data with same date to same reducer 
 */
public class BasicPartitioner extends
	Partitioner<TemperaturePair, NullWritable> {

	@Override
	public int getPartition(TemperaturePair key, NullWritable value,
			int numReduceTasks) {
		//System.out.println("HEYY PARTITIONER " + key.getDate());
		return (key.getDate().hashCode() % numReduceTasks);
	}
}