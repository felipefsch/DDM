package assignment3;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Assignment3 {
	
	public static String ordersTable = "files/ordersInput";
	public static String customersTable = "files/customersInput"; 
	public static String outputResult = "files/a3output";
	
	/**
	 * InnerJoin from:
	 * 
	 * customer(c custkey, c name, c address, c nationkey, c phone, c acctbal, c mktsegment, c comment)
	 * -line: 1|Customer#000000001|IVhzIApeRb ot,c,E|15|25-989-741-2988|711.56|BUILDING|to the even, regular platelets. regular, ironic epitaphs nag e
	 * 
	 * orders(o orderkey, o custkey → customer.c custkey, o orderstatus, o totalprice, o orderdate, o orderpriority, o clerk, o shippriority, o comment)
	 * -line: 1|36901|O|173665.47|1996-01-02|5-LOW|Clerk#000000951|0|nstructions sleep furiously among
	 * 
	 * Join by first field of customer and second field of order (customer.c_kustkey)
	 * 
	 * OUTPUT:
	 * (OrderId, OrderStatus, Price, customers->CustomerId, CustomerId, CustomerName)
	 */
 
	 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		        
		 	private static HashMap<String, String> customerMap = new HashMap<String, String>();
			private BufferedReader brReader;
			private String customerInfo = "";
			private Text txtMapOutputKey = new Text("");
			private Text txtMapOutputValue = new Text("");
			
			// Setup the program to create the CacheFile with customers
			@Override
			protected void setup(Context context) throws IOException,
					InterruptedException {
		 
				//Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());

				// context.getConfiguration() was returning NULL. Path HardCoded to solve it
				hashCustomers(new Path(customersTable), context);		 
			}
		 
			// Create hash of customers to join with orders
			private void hashCustomers(Path filePath, Context context) throws IOException {
				
				String strLineRead = "";
		 
				try {
					brReader = new BufferedReader(new FileReader(filePath.toString()));
		 
					// Read each line, split and load to HashMap
					while ((strLineRead = brReader.readLine()) != null) {
						
						String customerFields[] = strLineRead.split("\\|");
						String customerId = customerFields[0];
						String customerInfo = "";
						
						// TODO simplified for better visualization!! Swap FOR
						// Create Map value of hash as all the remaining fields of the customer entry
						//for (int i = 0; i < customerFields.length; i++)
						for (int i = 0; i < 2; i++)
							customerInfo += " " + customerFields[i]; 

						customerMap.put(customerId, customerInfo);
					}
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}finally {
					if (brReader != null) {
						brReader.close();
		 
					}		 
				}
			}
		 
			@Override
			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		 
				if (value.toString().length() > 0) {
					String orderAttributes[] = value.toString().split("\\|");
					String orderCustomerId = orderAttributes[1];
		 
					String orderId = orderAttributes[0];
					String orderAtt = "";
					
					// TODO simplified for better visualization!! Swap FORs
					//for (int i = 1; i < orderAttributes.length; i++)
					for (int i = 2; i < 4; i++) {
						orderAtt += " " + orderAttributes[i];
					}
					orderAtt += " " + orderAttributes[1];
					
					try {
						customerInfo = customerMap.get(orderCustomerId);
					} finally {
						customerInfo = ((customerInfo.equals(null) || customerInfo.equals("")) ? "NOT-FOUND" : customerInfo);
					}
					
					txtMapOutputKey.set(orderId);
		 
					txtMapOutputValue.set(orderAtt + " " + customerInfo);		 
				}
				context.write(txtMapOutputKey, txtMapOutputValue);
				customerInfo = "";
			}
		 }
		 
		@SuppressWarnings("deprecation")
		public static void main(String[] args) throws Exception {			
			Configuration conf = new Configuration();
			 	
		 	Job job = new Job(conf, "Assignment3 - MapSideJoin");
		 	
		 	// Data in cache to be joined
			DistributedCache.addCacheFile(new URI(customersTable),conf);
			
			//job.setJarByClass(DriverMapSideJoinDCacheTxtFile.class);
			FileInputFormat.setInputPaths(job, new Path(ordersTable));
			FileOutputFormat.setOutputPath(job, new Path(outputResult));
			job.setMapperClass(Map.class);
	 
			job.setNumReduceTasks(0);

			job.waitForCompletion(true);
		 }
	
}
