package assignment2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * This groups values by natural key (date) in the reducer
 */
public class GroupingComparator extends WritableComparator {
	  protected GroupingComparator() {
			super(TemperaturePair.class, true);
		}
	 
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			
			TemperaturePair key1 = (TemperaturePair) w1;
			TemperaturePair key2 = (TemperaturePair) w2;
			
			return key1.getDate().compareTo(key2.getDate());
		}
	}
