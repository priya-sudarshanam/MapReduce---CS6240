package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HCompute {
	//declare variables
	private static String tablename = "FLIGHT_DELAY";
	private static String delay = "MINUTESDELAY";
	private static byte[] DELAYFAMILY = Bytes.toBytes(delay);
	private static byte[] DELAYCOLUMN = Bytes.toBytes("delayMinutes");
	private static byte[] CANCELLEDCOLUMN = Bytes.toBytes("cancelled");
	
	public static class ComputeMapper extends TableMapper<Text, Text>{
		private Text flightKey = new Text();
		private Text flightVal = new Text();
		@Override
		public void map(ImmutableBytesWritable rowkey, Result values, Context context) 
				throws IOException, InterruptedException{
			//retrieve the variables from the key and values
			String year = Bytes.toString(rowkey.get()).substring(0,4);
			String month = Bytes.toString(rowkey.get()).substring(4,6);
			String airlineID = Bytes.toString(rowkey.get()).substring(6,11);
			String cancelledValue;
			String delayValue;
			Double newdelayValue = 0.00;
		 
		    //filter the records where year is not 2008
			if (!year.equals("2008"))
				return;
			
			//get rid of "0" in front of month
			if(month.startsWith("0"))
				month = month.substring(1);
			
			try {
			    //check if cancelledValue is null or empty, and equal to 0.00
				cancelledValue = new String(values.getValue(DELAYFAMILY,CANCELLEDCOLUMN));
				if (cancelledValue  != "") 
					if (!(cancelledValue.equals("0.00")))
						   	return;
				try {
                  //check if delayminutes is null or empty, if yes set it to 0.00				
				  delayValue = new String(values.getValue(DELAYFAMILY,DELAYCOLUMN));
				   if (delayValue != null || !delayValue.isEmpty())
				     newdelayValue = Double.parseDouble(delayValue);
				    keyOut.set(airlineID);
				    valOut.set(month + "," + newdelayValue);
				    context.write(keyOut, valOut);
				   
				   } catch (Exception e) {
				      newdelayValue = 0.00;
				   }
				
	            } catch (NumberFormatException e) {
	            e.printStackTrace();
	            }					
			
		}
	}
	
	public static class ComputeReducer extends Reducer<Text, Text, NullWritable, Text>{
		private static final int Null = 0;
		private NullWritable nullkey = NullWritable.get();
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException{
			//declare variables
			String flightString = key.toString();
			double sum[] = new double[12];
			Arrays.fill(sum, 0.00);
			int count[] = new int[12];
			double average[] = new double[12];
			int month, newAvg;
			
			//calculate the sum of the delay minutes and count of delay per month
			for(Text val: values){
				month = Integer.parseInt(val.toString().split(",")[0]);
				sum[month] += Double.parseDouble(val.toString().split(",")[1]);
				count[month] ++;
			}
			//calculate the average delay in each month, 
			//if there's no record in one month, set the average to 0
			for(int i = 0; i < 12; i++){
				if (count[i] != 0){
					average[i] = sum[i]/count[i];
					newAvg = (int) Math.round(average[i]);
				} else { newAvg = 0; }	
			//append each average to the output string
			flightString += ", (" + (i+1) + "," + newAvg + ")";
				
					
			}
			//emit the nullkey and values
			context.write(nullkey, new Text(flightString));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 1) {
			System.err.println("Usage: compute <out>");
			System.exit(1);
		}
		//set the scan 
		Scan scan = new Scan();
		scan.setCaching(500);
		//set the 'setCacheBlocks' to false prevents burdening the RegionServer 
		scan.setCacheBlocks(false);
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Compute Flight Delay");

		job.setJarByClass(HCompute.class);
	    TableMapReduceUtil.initTableMapperJob(tablename, scan,
				ComputeMapper.class, Text.class, Text.class, job);
		job.setNumReduceTasks(10);
		job.setReducerClass(ComputeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
		int exitCode = job.waitForCompletion(true) ? 0 : 1;
		System.exit(exitCode);
	}
}