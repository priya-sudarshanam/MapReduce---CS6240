package KMeans;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class KMeans
{
  public static class KMeansMapper extends Mapper<Object, Text, NullWritable, Text>
  {
    private static String strReputation, strCreationDate,strViews, strUpVotes,
                 strDownVotes, newDate, outString;
    private static Map<String,String> parsed;
	  
    public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException
    {
      parsed = KMeansUtils.convertFromXMLtoString(value.toString());
      strReputation = (String)parsed.get("Reputation");
      strCreationDate = (String)parsed.get("CreationDate");
      strViews = (String)parsed.get("Views");
      strUpVotes = (String)parsed.get("UpVotes");
      strDownVotes = (String)parsed.get("DownVotes");
    
      try {
    	  if (strReputation != null && strViews !=  null && strUpVotes != null &&
    			  strDownVotes != null && strCreationDate != null) { 
    		  
    		  String strCDate = strCreationDate.substring(0, 10);
    		  newDate = strCDate.substring(0,4)+strCDate.substring(5,7)+
     				 strCDate.substring(8,10);
    		  outString = strViews+ ","+ newDate + ","+
     				   strUpVotes + "," + strDownVotes + ","+ strReputation;
    		  context.write(NullWritable.get(), new Text(outString));
    	    
          }
    	  
      }catch (Exception e) {
	     strReputation = "0";
	      strViews= "0";
	      strUpVotes = "0";
	      strDownVotes = "0";
	      strCreationDate = "0";
    }
  }
  }
  

  
  public static void main(String[] args) throws Exception
  {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2)
    {
      System.err.println("Usage: KMeans <inXML> <outTEXT>");
      System.exit(2);
    }
    Job job = new Job(conf, "Min Max");
    job.setJarByClass(KMeans.class);
    job.setMapperClass(KMeansMapper.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    boolean result = job.waitForCompletion(true);
  }
}
