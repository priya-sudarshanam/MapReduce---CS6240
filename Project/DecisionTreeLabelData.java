package LabelUser;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LabelUser
{
  public static class LabelUserMap extends Mapper<Object, Text, NullWritable, Text>
  {
    private Text outText = new Text();
	private static int reputationMedian = 7037, viewsLow = 2, viewsMiddle = 8, upvotesLow = 6,
                    upvotesMiddle = 31,downvotesLow = 10, downvotesMiddle = 17, creationDateLow = 20110901;
    private static String line, outString;
    private static String[] splitLine;
    private static int viewsData, upVotesData, downVotesData, reputationData;
    
    private String rlabel = "", vlabel = "", uplabel="", downlabel="", cdatelabel="";
    
    public void map(Object key, Text value, Mapper<Object, Text, NullWritable, Text>.Context context)
      throws IOException, InterruptedException
    {
    	   
    	line = value.toString();
		splitLine = line.split(",");
		viewsData= Integer.parseInt(splitLine[0]);
		upVotesData = Integer.parseInt(splitLine[2]);		
		downVotesData = Integer.parseInt(splitLine[3]);
		reputationData = Integer.parseInt(splitLine[4]);
		
		//VIEWS		
		if (viewsData <= viewsLow)
			vlabel = "vLow";
		else if (viewsData <= viewsMiddle)
			vlabel = "vMedium";
		else 
			vlabel = "vHigh";

		//CREATION DATE
		if (Integer.parseInt(splitLine[1]) <= creationDateLow)
			cdatelabel = "Past";
		else 
			cdatelabel = "Present";

		//UPVOTES
		if (upVotesData <= upvotesLow)
			uplabel = "uLow";
		else if (upVotesData <= upvotesMiddle)
			uplabel = "uMedium";
		else 
			uplabel = "uHigh";
		//DOWNVOTES
		if (downVotesData <= downvotesLow)
			downlabel = "dLow";
		else if (downVotesData <= downvotesMiddle)
			downlabel = "dMedium";
		else 
			downlabel = "dHigh";
		
		//REPUTATION
		if (reputationData <= reputationMedian)
			rlabel = "rLow";
		else
			rlabel = "rHigh";
	        
    outString = vlabel+ ","+ cdatelabel + ","+ uplabel + "," + downlabel + ","+ rlabel ;
    outText.set(outString);
    context.write(NullWritable.get(), outText);
    }
  }
  

  public static void main(String[] args) throws Exception
  {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2)
    {
      System.err.println("Usage: Label Data <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "Label Data");
    job.setJarByClass(LabelUser.class);
    job.setMapperClass(LabelUserMap.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    boolean result = job.waitForCompletion(true);
    
  }
}
