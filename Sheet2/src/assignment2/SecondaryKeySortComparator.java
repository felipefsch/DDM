package assignment2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondaryKeySortComparator extends WritableComparator {
	 
	  protected SecondaryKeySortComparator() {
			super(TemperaturePair.class, true);
		}
	 
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			TemperaturePair key1 = (TemperaturePair) w1;
			TemperaturePair key2 = (TemperaturePair) w2;
			
			int cmpResult = key1.getDate().compareTo(key2.getDate());
			if (cmpResult == 0)// same date
			{
				Double t1 = Double.parseDouble(key1.getTemperature());
				Double t2 = Double.parseDouble(key2.getTemperature());
				return -t1.compareTo(t2);
				//Use the minus to order in descending order
			}
			return cmpResult;
		}
	}