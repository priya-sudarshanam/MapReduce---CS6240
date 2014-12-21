package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import au.com.bytecode.opencsv.CSVParser;

public class HPopulate {
	
	private static String tablename = "FLIGHT_DELAY";
	private static String delayFamily = "MINUTESDELAY";
	private static String delayTitle = "delayMinutes";
	private static String cancelledTitle = "cancelled";
	
	public static class PopulateMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
	    //declare variables
		Configuration config;
		HTable table;
		List<Put> puts;

		@SuppressWarnings("deprecation")
		protected void setup(Context context) throws IOException,
				InterruptedException {
			//initialize hbase, table and the record list
			config = HBaseConfiguration.create();
			table = new HTable(config, tablename);
			table.setAutoFlush(false);			
			puts = new LinkedList<Put>();
		}
		
			
		@Override
		public void map(LongWritable keyin, Text value, Context context) 
				throws IOException, InterruptedException{
			//retrieve the needed value from the current line
			CSVParser parser = new CSVParser();
			String nextLine[] = parser.parseLine(value.toString());
			int year = Integer.parseInt(nextLine[0]);
			String month = nextLine[2], airlineId = nextLine[7], flightDelay =nextLine[37],
		            isCancelled = nextLine[41];
			
			//pad the month as it is the key
			if (!(month.length() == 2)){
				month = "0" + month;
			}
			
			//check if the flightdelay is null or empty.
			//if yes the set it to 0.00
			if (flightDelay == null || flightDelay.isEmpty())
				flightDelay ="0.00";			
									
			//generate rowkey 
			//including the nanotime makes it unique
			  String rowString = year + month + airlineId + System.nanoTime();
			  byte[] rowkey = rowString.getBytes();
			  byte[] flightRowKey = RowKeyConverter.makeflightRowKey(rowkey, year);
			    Put p;
				p = new Put(flightRowKey);
				//create the column family and add delayminutes and cancelled column to the family
				p.add(Bytes.toBytes(delayFamily), Bytes.toBytes(delayTitle), Bytes.toBytes(flightDelay));
				p.add(Bytes.toBytes(delayFamily), Bytes.toBytes(cancelledTitle),  Bytes.toBytes(isCancelled));
				//add each record to the puts variable
				puts.add(p);
			  
			}
		
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			//add the record to the table and close
			table.put(puts);
			table.close();
               }
		}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 1) {
			System.err.println("Usage: populate <in>");
			System.exit(1);
		}
		//create table
		HBaseAdmin admin = new HBaseAdmin(conf);
		@SuppressWarnings("deprecation")
		HTableDescriptor htd = new HTableDescriptor(tablename);
		HColumnDescriptor hcd = new HColumnDescriptor(delayFamily);
		htd.addFamily(hcd);
		//if table exists, drop it
		if(admin.tableExists(tablename)){
			admin.disableTable(tablename);
			admin.deleteTable(tablename);
		}
		//create the table and close it
		admin.createTable(htd);
		admin.close();
		//set the output to the table format
		conf.set(TableOutputFormat.OUTPUT_TABLE, tablename);
		Job job = new Job(conf, "populate htable");
		job.setJarByClass(HPopulate.class);
		job.setMapperClass(PopulateMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
	    job.setOutputFormatClass(TableOutputFormat.class);

	   	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		int exitCode = job.waitForCompletion(true) ? 0 : 1;
		
	}
}