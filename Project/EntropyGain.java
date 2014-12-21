package DTree;

import java.io.*;
import java.util.*;
import java.lang.Math;
 class EntropyGain
 {
	  public static int currIndex,i,j, ind, getMax, sum;
	  public static int[] entData;
	  public static double entropy;
	  int num=0;
	  static String total[][]=new String[1000][4];
	  int currAttr[]=new int[100];
	  public static String majorityLabel="", classLabel, line;
	  public static DataInputStream in;
	  public static BufferedReader br;
	  public static  StringTokenizer itr;		
	  
	  public String majorityLabel() {
		  return majorityLabel;
	  }

	//Calculation of entrophy
	  public double currNodeEntophy() {
		  currIndex=0; entropy=0;i=0;
		  currIndex=Integer.parseInt(total[0][0]);
		  entData=new int[1000];
		  classLabel=total[0][2];
		  j=0; ind=-1; getMax=0;
		  while(currIndex==Integer.parseInt(total[j][0])) {
			  if(entData[j]==0) {
				  classLabel=total[j][2];
				  	ind++;
		            i=j;
				  while(currIndex==Integer.parseInt(total[i][0])) {
					  if(entData[i]==0) {
			  				if(classLabel.contentEquals(total[i][2])) {
			  					currAttr[ind]=currAttr[ind]+Integer.parseInt(total[i][3]);
			  					entData[i]=1;}			  			
				  		}
		  			i++;
		  			if(i==num)
						  break;
				  }
				  if(currAttr[ind]>getMax)  {
					  getMax=currAttr[ind];
					  majorityLabel=classLabel;
				  }
				 }  else {
				j++;
			  }
			  if(j==num)
				  break;
		  
		  }
		  entropy=calcEntropy(currAttr);
		  return entropy;
		  	  }
	  
	  public double calcEntropy(int c[]){
		  entropy=0;i=0; sum=0;
		  double frac;
		  while(c[i]!=0) {
			 sum += c[i];
			 i++;
		  }
		  i=0;
		  while(c[i]!=0) {			  
			  frac=(double)c[i]/sum;		  
			  entropy -= frac*(Math.log(frac)/Math.log(2));
			  i++;
		  }		
		  return entropy;
	  }
	  
	  public void readData() { 
	  String intFile = Entropy.otherArgs[1];
	  FileInputStream fstream;
	   try {		
		fstream = new FileInputStream("./" + intFile + "/intermediate" + Entropy.currIndex + ".txt");
			
		  in = new DataInputStream(fstream);
		  br = new BufferedReader(new InputStreamReader(in));
		  while ((line = br.readLine()) != null){
			  for (int j=0;j<4;j++){
				  itr= new StringTokenizer(line);
				  total[num][j]=itr.nextToken();
				  total[num][j]=itr.nextToken();
				  total[num][j]=itr.nextToken();
				  total[num][j]=itr.nextToken(); }
			  	  int i=num;
			
			  num++;
		    }
		  total[num][0]=null;
		  total[num][1]=null;
		  total[num][2]=null;
		  total[num][3]=null;
		  in.close();
		  
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    	  
	    	  //Close the input stream
	}
	  
  }  
	  
	  public double gainratio(int index,double enp) {
		  int c[][]=new int[1000][100];
		  int sum[]=new int[1000]; //
		  String currentatrrval="";
		  double gainratio=0;
		  int j=0, m=-1, lines=num, totalsum=0;
		  for(int i=0;i<lines;i++) {
			  if(Integer.parseInt(total[i][0])==index) {
	    		  if(total[i][1].contentEquals(currentatrrval)) {
	    		  j++;
	    		  c[m][j]=Integer.parseInt(total[i][3]);
	    		  sum[m]=sum[m]+c[m][j];
	    		  } else {
	    			  j=0;
	    			  m++;
	    			  currentatrrval=total[i][1];
	    			  c[m][j]=Integer.parseInt(total[i][3]); 
	    			  sum[m]=c[m][j];
	    		  }
	    	       
			  }
		  }
		  int p=0;
		  while(sum[p]!=0) {
		  totalsum=totalsum+sum[p];
		  p++;
		  }
		
		  double wtenp=0;
		  double splitenp=0;
		  double part=0;
		  for(int splitnum=0;splitnum<=m;splitnum++) {
			  part=(double)sum[splitnum]/totalsum;
			 wtenp += part*calcEntropy(c[splitnum]);
		  }
		  splitenp=calcEntropy(sum);
		  gainratio=(enp-wtenp)/(splitenp);
		  return gainratio;
		  }	  
	  
	  public String getvalues(int n){   
		     int flag=0;
			 String values="";
			 String temp="";
			 for(int z=0;z<1000;z++){
			   if(total[z][0]!=null) {
			    if(n==Integer.parseInt(total[z][0])){
				  flag=1;				
				  if(!total[z][1].contentEquals(temp)) {
					 values += " "+total[z][1];
					 temp=total[z][1];
	         	 }				 
			 } else if(flag==1)
				break; }
			else
				break;
			 }
			 return values;
			 
		 }
	  
}
	  