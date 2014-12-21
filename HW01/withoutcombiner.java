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
 public static class TokenizerMapper  extends Mapper<Object, Text, Text, IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
  FILTER OUT THE WORDS BEGINNING WITH EITHER M,N,O,P OR Q
        //check if the word starts with either m,n,o,p or q
        //if they do, then output them to the reducer
        if (word.toString().toLowerCase().matches("^[mnopq].*")){
           context.write(word, one);   }    }     }   }
PARTITIONER CLASS
   //partitioner class that partitions that partitions the data depending on whether 
  //the word starts with m,n,o,p,q
  //This extends the base class Partitioner
  public static class WordPartitioner extends Partitioner<Text, Text> {
      @Override
      public int getPartition(Text key, Text value, int numReduceTasks) {
          //check to avoid performing mod with 0
          if(numReduceTasks == 0)  return 0;
          //if the word begins with 'm' or 'M', assign partition 0
          if(key.toString().toLowerCase().matches("^[m].*")){               
              return 0;      }
          //else if the word begins with 'n' or 'N', assign partition 1
          if(key.toString().toLowerCase().matches("^[n].*")){             
              return 1 % numReduceTasks;  }
        //else if the word begins with 'o' or 'O', assign partition 2
          if(key.toString().toLowerCase().matches("^[o].*")){  return 2 % numReduceTasks;  }
        //else if the word begins with 'p' or 'P', assign partition 3
          if(key.toString().toLowerCase().matches("^[p].*")){ return 3 % numReduceTasks;  }
          //otherwise assign partition 4
           Else  return 4 % numReduceTasks;   }  }
  
  public static class IntSumReducer   extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values,  Context context  )
                               throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) { sum += val.get();  }
      result.set(sum);
      context.write(key, result);  }   }
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
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
