package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import au.com.bytecode.opencsv.CSVParser;

public class Secondary {
	public static class FlightMapper extends Mapper<LongWritable, Text, IntPair, DoubleWritable>{
		private IntPair outputKey = new IntPair();
		private DoubleWritable outputValue = new DoubleWritable();
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
				
			//parse and retrieve the needed values from each line
			CSVParser parser = new CSVParser();
			String flightRecord[] = parser.parseLine(value.toString());
			int flightMonth = Integer.parseInt(flightRecord[2]),				
				flightAirlineID = Integer.parseInt(flightRecord[7]);
			String flightArrDelayMinutes = flightRecord[37];
			float flightCancelled = Float.parseFloat(flightRecord[41]);
			int flightYear = Integer.parseInt(flightRecord[0]);
			Double flightDelay;
			
			//check if the delay is empty
			if (flightArrDelayMinutes.isEmpty())
						flightDelay = 0.00;
			else 
				flightDelay = Double.parseDouble(flightArrDelayMinutes);
			
			//filter out the records that actually departed in 2008 and are not cancelled
			if (flightYear == 2008 && flightCancelled == 0.00){
				outputKey.setAirline(flightAirlineID);
				outputKey.setMonth(flightMonth);				
				outputValue.set(flightDelay);
				//set the key and value and output them
				context.write(outputKey, outputValue);
			}
		}
	}
	//compare the key 
	public static class FlightKeyComparator extends WritableComparator{
		protected FlightKeyComparator(){
			super(IntPair.class,true);
		}
		@Override
		public int compare(WritableComparable tp1, WritableComparable tp2){
			IntPair ip1 = (IntPair) tp1;
			IntPair ip2 = (IntPair) tp2;
			int cmp = ip1.compareTo(ip2);
			if(cmp !=0){
				return cmp;
			}
			return ip1.monthCompare(ip2);
		}
	}
	
	//partition the data based on the airlineid
	public static class FlightPartitioner extends Partitioner<IntPair, DoubleWritable>{

		@Override
		public int getPartition(IntPair key, DoubleWritable value, int numPartitions) {
			return Math.abs(key.getAirline() *127) % numPartitions;
		}
	}
	
	//group data based on the key
	public static class FlightGroupingComparator extends WritableComparator{
		public FlightGroupingComparator(){
			super(IntPair.class, true);
		}
		public int compare(WritableComparable tp1,WritableComparable tp2){
			IntPair ip1 = (IntPair) tp1;
			IntPair ip2 = (IntPair) tp2;
			return ip1.compareTo(ip2);
		}
	}
	
	public static class FlightReducer extends Reducer<IntPair, DoubleWritable, NullWritable, Text>{
		private NullWritable nullkey = NullWritable.get();
		
		public void reduce(IntPair key, Iterable<DoubleWritable> values, Context context) 
				throws IOException, InterruptedException{
			String outString = "";
			//get the first value
			Iterator<DoubleWritable> it = values.iterator();
			Double nextDelay = it.next().get();
			int currMonth = key.getMonth();
			int nextMonth = key.getMonth();
			double sum = nextDelay;
			int count = 1;
			int average;
			while(it.hasNext()){
				nextDelay = it.next().get();
				nextMonth = key.getMonth();
				//check if the currMonth is same as the nextMonth.
				//If yes, then add the value to sum, and add count
				if(nextMonth == currMonth){
					sum += nextDelay;
					count ++;
				//else calculate the current average and
				//initialize the sum, count and currMonth to the start
				} else {
					//average = (int)Math.ceil(sum / count);
					outString += ", (" + currMonth + "," + (int)Math.ceil(sum / count) + ")";
					sum = nextDelay;
					count = 1;
					currMonth = nextMonth;
				}
			}
			//calculate the average delay for each month and emit it
			outString += ", (" + currMonth + "," + (int)Math.ceil(sum / count) + ")";
			outString = key.getAirline() + outString;
			while(currMonth < 12){
				outString += ",(" + (++ currMonth) + ",NULL)";
			}
			//emit the key and value
			context.write(nullkey, new Text(outString));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: Monthly Delay <in> <out>");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "Monthly Delay");
		//set the number of reduce tasks to 10 for load balancing
	    job.setNumReduceTasks(10);
	    job.setPartitionerClass(FlightPartitioner.class);
	    job.setSortComparatorClass(FlightKeyComparator.class);
	    job.setGroupingComparatorClass(FlightGroupingComparator.class);
	    job.setJarByClass(Secondary.class);
	    job.setMapperClass(FlightMapper.class);
	    job.setReducerClass(FlightReducer.class);
	    job.setOutputKeyClass(IntPair.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}