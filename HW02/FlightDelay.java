package org.apache.hadoop.examples;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FlightDelay {
	
	//Global Counter where delaySum sums the delays
	// and totalFlights sums the total number of flights
	public enum flightCounters {
		delaysum,totalFlights
	}

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			// Split the data  on "," to extract the required fields
			String[] record = value.toString().split(",");

			//Assign the required fields to a variable and store
			String flightDate = record[5], origin = record[11], destCity = record[18],
			departureTime = record[26],arrivalTime = record[37],delay = record[39],
			cancelled = record[43],diverted = record[45],flightMonth = record[2],flightYear=record[0];

			// Replace the inverted commas present in the string  eg "JFK", "ORD"
			origin=origin.replace("\"", "");
			destCity=destCity.replace("\"", "");
			
			boolean checker = false;
			try {
				//check whether the flight is between (june and year 2007) and (may and 2008)
				checker = ((Integer.parseInt(flightMonth)>=06 && flightYear.equals("2007"))
						|| (Integer.parseInt(flightMonth)<=05 && flightYear.equals("2008")));
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			// The initial check for the figuring if the plane took off from "ORD" 
			// and not land at" JFK"
			Boolean flightCheckLeg1=(origin.equals("ORD") && !destCity.equals("JFK"));
			
			// The initial check for the figuring if the plane landed at "JFK" 
			// and did not take off from "ORD"
			Boolean flightCheckLeg2=(!origin.equals("ORD") && destCity.equals("JFK"));
			
			// Additional check to see if flight wasn't diverted of or cancelled
			if ((flightCheckLeg1 || flightCheckLeg2) && checker && 
					cancelled.equals("0.00") && diverted.equals("0.00")) {
				Text Key;

				//Key as a combination of the destCity/origincity-flightDate
				if (origin.equals("ORD"))
					Key = new Text(destCity + "-" + flightDate);
					Text val = new Text(origin + "-" + departureTime+ "-" + delay);
				else
					Key = new Text(origin + "-" + flightDate);
					Text val = new Text(origin + "-" + arrivalTime + "-" + delay);
						
				//emit(key,value)
				context.write(Key, val);
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, Text, Text, FloatWritable> {
		private FloatWritable result = new FloatWritable();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			//initialize two vectors to store the details of the two legs of the flight
			Vector<String> info1 = new Vector<String>();
			Vector<String> info2 = new Vector<String>();
			
			/// flightLeg1 stores the flights from ORD,  flightLeg2 stores the flight not from ORD
			for (Text val : values) {
				String x = val.toString();
				String[] record = x.split("-");
				if (record[0].equals("ORD")) {
					info1.add(x);
				} else {
					info2.add(x);
				}
			}
			
			// check if the arrivalTime is lesser than the departureTime
			String[] outer,inner;
			for (int i = 0; i < info1.size(); i++) {
				outer = info1.get(i).split("-");
				for (int j = 0; j < info2.size(); j++) {
					inner = info2.get(j).split("-");
					// The intial check is for entry strings 
					if (!(outer[2].trim().length() == 0 
							|| inner[1].trim().length() == 0)
									&& (Integer.parseInt(outer[2].replace("\"", "")) 
											< Integer.parseInt(inner[1].replace("\"", "")))) {
						// Update the global counter with new information
						context.getCounter(flightCounters.delaysum).increment
						((long) (Float.parseFloat(outer[3]) + Float.parseFloat(inner[3])));
						context.getCounter(flightCounters.totalFlights).increment(1);
					}
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "Flight Delay");
		job.setJarByClass(FlightDelay.class);
		job.setMapperClass(TokenizerMapper.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setNumReduceTasks(10); // Setting number of reduce tasks to 10
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		boolean result = job.waitForCompletion(true);
		
		// get counters and display average
		Counters counters = job.getCounters();
		float totalDelay= counters.findCounter(flightCounters.delaysum).getValue();
		float totalCountofFlights = counters.findCounter(flightCounters.totalFlights).getValue();
		Log log = LogFactory.getLog(FlightDelay.class);
		log.info("Average Delay: "+(totalDelay/totalCountofFlights));
		log.info("Total Number of Flights: "+(totalCountofFlights)); 

		

	}
}


