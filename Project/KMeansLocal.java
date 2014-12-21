package KNNLocal;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KNNLocal {
	
	private static final int DIMENSION =5;
	private static int CLUSLIMIT = 2;
	private static final String SPLIT = ",";
	private static double[][] curCentroids;
	private static double[][] newCentroids;
	private static final double STOPLIMIT = 0.0001;
	private static List<double[]> points = new ArrayList<double[]>();
	private static double dist, sum;
	private static String[] doubleString ;
	private static double[] kInts;
	private static BufferedReader br;
	private static String line;
	
	//read all the points from the file
	@SuppressWarnings("resource")
	public static void initPoints(String filename){
		try {
			br = new BufferedReader(new FileReader(filename));
			line = "";
			while((line = br.readLine()) != null){
				points.add(StrToInteger(line));
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static double[] StrToInteger(String s) {
		doubleString = s.split(SPLIT);
		kInts = new double[DIMENSION + 1];
		for (int i = 0; i < DIMENSION; i++) {
			kInts[i] = Integer.parseInt(doubleString[i]);
		}
		return kInts;
	}
	
	public static void initCentroids(){
		curCentroids = new double[CLUSLIMIT][DIMENSION];
		newCentroids = new double[CLUSLIMIT][DIMENSION];
		for(int i = 0; i < CLUSLIMIT; i ++){
			newCentroids[i] = points.get(i);
			Arrays.fill(curCentroids[i], 0);
		}

	}
	
	public static boolean hasConverged(){
		for(int i = 0; i < CLUSLIMIT; i ++){
			dist = distance(curCentroids[i], newCentroids[i]);
			if(dist > STOPLIMIT){
				return false;
			}
		}
		return true;
	}
	
	public static double distance(double[] p1, double[] p2){
		sum = 0.0;
		for(int i = 0; i < DIMENSION; i ++){
			sum += Math.pow(Math.abs(p1[i] - p2[i]), DIMENSION);
		}
		return Math.pow(sum, 1d / DIMENSION);
	}
	
	public static void updateCentroids(){
		for(int i = 0; i < CLUSLIMIT; i ++){
			for(int j = 0; j < DIMENSION; j ++){
				curCentroids[i][j] = newCentroids[i][j];
			}
			
		}
	}
	
	//find the nearest centroids for all the points
	public static void nearestCentroids(){
		double dist;
		for(double[] p : points){
			double min = Double.MAX_VALUE;
			for(int i = 0; i < CLUSLIMIT; i ++){
				dist = distance(p, curCentroids[i]);
				if(dist < min){
					min = dist;
					p[DIMENSION] = (int) i;
				}
			}
		}
	}
	
	//calculate the new centroids
	public static void calcNewCentroids(){
		int i = 0;
		int j = 0;
		double[][] sum = new double[CLUSLIMIT][DIMENSION];
		double[] count = new double[CLUSLIMIT];
		for(i = 0; i < CLUSLIMIT; i ++){
			count[i] = 0.0;
			for(j = 0; j < DIMENSION; j ++){
				sum[i][j] = 0.0;
			}
		}
		
		int index;
		for(double[] p : points){
			index = (int) p[DIMENSION];
			for(i = 0; i < DIMENSION; i ++){
				sum[index][i] += p[i];
			}
			count[index] ++;
		}
		
		for(i = 0; i < CLUSLIMIT; i ++){
			for(j = 0; j < DIMENSION; j ++){
				newCentroids[i][j] =  (sum[i][j] / count[i]);
			}
		}
	}
	
	public static double squaredError(){
		double squaredError = 0.0;
		int index;
		for(double[] p : points){
			index = (int) p[DIMENSION];
			squaredError += Math.pow(distance(p, newCentroids[index]), 2.0);
		}
	
		return squaredError;
	}
 
	public static void main(String[] args) {
		if(args.length != 2){
			System.err.println("Usage: Kmeans <in> <k>");
			System.exit(2);
		}
		long startTime = System.currentTimeMillis();
		CLUSLIMIT = Integer.parseInt(args[1]);
		initPoints(args[0]);
		initCentroids();
		while(!hasConverged()){
			updateCentroids();
			nearestCentroids();
			calcNewCentroids();
		}
		for(int i = 0; i < CLUSLIMIT; i ++){
			for(int j = 0; j < DIMENSION; j ++){
				System.out.print(curCentroids[i][j] + ",");
			}
			System.out.println();
		}
		
		long endTime = System.currentTimeMillis();
		System.out.println("Time:"+ Long.toString(endTime - startTime) + "\n\nSquared Error:" + squaredError());
		
	}

}
