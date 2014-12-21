package DTree;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Reducer;

public class Entropy extends Configured implements Tool{

	public static String[] otherArgs;
    public static AttributeData currSplit=new AttributeData();    
    public static List <AttributeData> splitData=new ArrayList<AttributeData>();    
    public static int currIndex, j,nodeData,tempSize,splitIndex,splitSize, total_attributes,splits;
    public static  String attrValSplit,dt, nextToken;
    public static StringTokenizer attrs;
    public static Job job;
    public static double gainratio, bestGainRatio,entropy;
    public static EntropyGain gainObj;
    public static AttributeData newnode;

    @SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
	  splitData.add(currSplit);	  
	  nodeData=0; splitIndex=0;splitSize=splitData.size();
	  gainratio=0;bestGainRatio=0;entropy=0;
	  total_attributes=4;
	  while(splitSize > currIndex){
    	currSplit=splitData.get(currIndex); 
    	gainObj=new EntropyGain();
    	 
	    nodeData = ToolRunner.run(new Configuration(), new Entropy(), args);	  
	    gainObj.readData();
	    entropy=gainObj.currNodeEntophy();
	    currSplit.classLabel=gainObj.majorityLabel();
	    
    	if(entropy!=0.0 && currSplit.attrIndex.size()!=total_attributes) {
	    	System.out.println("Entropy for the current split: "+entropy);
	    	bestGainRatio=0;
	 
	        for(j=0;j<total_attributes;j++)	{	//Finding the entropy gain of each attribute
	          if(!currSplit.attrIndex.contains(j)) { 
	  	   	    gainratio=gainObj.gainratio(j,entropy);
		        if(gainratio>=bestGainRatio){
		  			splitIndex=j;
		  			bestGainRatio=gainratio;
		  		   }
	  	      }	  	      
	    }
	    
	    attrValSplit=gainObj.getvalues(splitIndex);
	    attrs = new StringTokenizer(attrValSplit);
	    splits=attrs.countTokens(); //number of splits possible with  attribute selected
	    nextToken="";
	    
	    for(int i=1;i<=splits;i++) {
	    	tempSize=currSplit.attrIndex.size();
	    	newnode=new AttributeData(); 
	    	for(int y=0;y<tempSize;y++) {	    	
	    		newnode.attrIndex.add(currSplit.attrIndex.get(y));
	    		newnode.attrVal.add(currSplit.attrVal.get(y));
	    	}
	    	nextToken=attrs.nextToken();	    	
	    	newnode.attrIndex.add(splitIndex);
	    	newnode.attrVal.add(nextToken);
	    	splitData.add(newnode);
	      }
	    } else {
	    	dt="";
	    	tempSize=currSplit.attrIndex.size();
	    	for(int val=0;val<tempSize;val++){
	    	dt += " "+currSplit.attrIndex.get(val)+" "+currSplit.attrVal.get(val);
	    	}
	    	dt += " "+currSplit.classLabel;
	    	try {	            
		    	BufferedWriter bw = new BufferedWriter(new FileWriter(new File("./DecisionTree.txt"), true));    
		    	bw.write(dt);
		            bw.newLine();
		            bw.close();
		       } catch (Exception e) {
		    }
	    }
	    splitSize=splitData.size();
	    currIndex++;     
        }
	  
	  System.out.println("Decision Tree printed");
	  System.exit(nodeData);

  }
   
  public int run(String[] args) throws Exception {
	 Configuration conf = this.getConf();
	   
	  job = new Job(conf);
	  job.setJobName("DecisionTree");
      otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
      if (otherArgs.length != 3) {
          System.err.println("Usage: DecisionTree <in><intermediate><out>");
          System.exit(2); }
      job.setJarByClass(Entropy.class);
      job.setMapperClass(DecisionTree.MapUser.class);
      job.setReducerClass(DecisionTree.ReduceUser.class);
	  job.setNumReduceTasks(1);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileInputFormat.addInputPath(job,new Path(otherArgs[1]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]+ "/output"+currIndex));
 
     return job.waitForCompletion(true) ? 0 : 1;
    
  }
  
  
  
}
