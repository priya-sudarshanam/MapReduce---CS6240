package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PriyaWC {
  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{    
	Map<Text, IntWritable> wordMap;
	Text word = new Text();
               IntWritable result = new IntWritable();
	@Override
    protected void setup(Context context) throws IOException, InterruptedException {
		wordMap = new HashMap<Text, IntWritable>();
    }
	@Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());        
        while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        //check if the word starts with either m,n,o,p or q
        if (word.toString().toLowerCase().matches("^[mnopq].*")) {
        	IntWritable count = wordMap.get(word);
    		if (count == null) {
    			// no entry exists, so create a new count for word
    			count = new IntWritable(0);
    			wordMap.put(new Text(word), count);
    		}      
    		 count.set(count.get() + 1);
    		 wordMap.put(word, count); } } }
    @Override
    protected void cleanup(Context context)
    		  throws IOException, InterruptedException {
    	for (Entry<Text, IntWritable> entry : wordMap.entrySet()) {
			context.write(entry.getKey(), entry.getValue()); }  }   }
  
  //partitioner class that partitions that partitions the data depending on whether 
  //the word starts with m,n,o,p,q
  //This extends the base class Partitioner
  public static class WordPartitioner extends Partitioner<Text,IntWritable> {	  
      @Override
      public int getPartition(Text key, IntWritable value, int numReduceTasks) {
          //check to avoid performing mod with 0
          if(numReduceTasks == 0)
              return 0;
          else 
              return (key.toString().toLowerCase().charAt(0)) % numReduceTasks;                   
   }   }
  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();  }
      result.set(sum);
      context.write(key, result);   }  } 
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "word count");
    job.setJarByClass(PriyaWC.class);
    job.setMapperClass(TokenizerMapper.class);
   // job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
   // job.setNumReduceTasks(3);
   // job.setPartitionerClass(WordPartitioner.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);  } }

