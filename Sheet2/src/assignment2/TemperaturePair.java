package assignment2;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

 public class TemperaturePair implements Writable, WritableComparable<TemperaturePair> {
	private String date;
	private String temperature;
	
	public TemperaturePair(){
	}
	
	public TemperaturePair(String dat, String temp){
		this.date = dat;
		this.temperature = temp;
	}
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		date = WritableUtils.readString(dataInput);
		temperature = WritableUtils.readString(dataInput);		
	}
	@Override
	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeString(dataOutput, date);
		WritableUtils.writeString(dataOutput, temperature);
	}
	@Override
	public int compareTo(TemperaturePair keyPair) {
		int comp = this.getDate().compareTo(keyPair.getDate());
		if (comp == 0)
			comp = this.getTemperature().compareTo(keyPair.getTemperature());
		
		return comp;
	}
	
	public void setDate(String date) {
		this.date = date;
	}
	
	public void setTemperature(String temperature) {
		this.temperature = temperature;
	}
	
	@Override
	public String toString() {
		return (this.date + " " + this.temperature);
	}
	
	public String getDate() {
		return this.date;
	}
	
	public String getTemperature() {
		return this.temperature;
	}
 }