package DTree;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class DecisionTree {
	public class MapUser extends Mapper <LongWritable, Text, Text, IntWritable> {		
		private final IntWritable one = new IntWritable(1);
		private Text attValue = new Text();
		private int i, index, sizeSplit;
		private String data, line, attrVal,label;
		private final Entropy id = new Entropy();
		private final AttributeData split = id.currSplit;
		
		
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
		  sizeSplit=0;line = value.toString();   
		  boolean check=true;
		  String[] x = line.split(" ");
		  index=0; attrVal= ""; 
		  String[] attr =new String[x.length-1];
		  label=x[x.length-1]; 
		  for(i =0;i<x.length-1;i++){
			  attr[i]=x[i];		
		  }
		    sizeSplit=split.attrIndex.size();
		  for(i=0;i<sizeSplit;i++) {
			  index=(Integer) split.attrIndex.get(i);
			  attrVal=(String)split.attrVal.get(i);
			 if(!attr[index].equals(attrVal)){ 
				 check=false;
				 break;
			 }	   
		  }  
		  if(check) {
			  for(int l=0;l<4;l++) {  
				  if(!split.attrIndex.contains(l)) {
					  data=l+" "+attr[l]+" "+label;
					  attValue.set(data);
					  context.write(attValue, one);
				  }		   
		  	}
			  if(sizeSplit==x.length-1) {
				  data=(x.length-1)+" "+"null"+" "+label;
				  attValue.set(data);
				  context.write(attValue, one);
			  	}
		   }
		 }
		  
		}
		
		
	public  class ReduceUser extends Reducer <Text, IntWritable, Text, IntWritable> {	
	    
	public void reduce(Text key, Iterator<IntWritable> values, Context context) throws IOException {
	   int sum = 0;
	   while (values.hasNext()) {
	     sum += values.next().get();
	  }
		try {
		   	String intFile = Entropy.otherArgs[1];
		    BufferedWriter bw = new BufferedWriter(new FileWriter(new File("./" + intFile + "/intermediate" + Entropy.currIndex + ".txt"), true));
		    bw.write(key+" "+sum);
		    bw.newLine();
		    bw.close();
		    } catch (Exception e) {
		    }
	      }

	 }
		
}