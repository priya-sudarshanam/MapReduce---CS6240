package KMeansMR;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class Utils {

	public static final int DIMENSION = 5;
	private static final double convergedPoint = 0.0001;
	private static double sum, dist;
	private static final String SPLIT = ",";
	private static double[] p1, p2, doubles, center;
	private static String[] doubleString, currCenters, updatedCenters;
	private static StringBuffer sb;
	private static int l, KNum, k ;
	private static String uriIn, uriOut,index, centerString,fName, uri, dPointPath, currClusters, updatedClusters, line;
	private static FileSystem fsCurClus, fsPoint,fsUpdated, fsOut,fsIn;
	private static Path pathCurClus, pathUpdate, current;
	private static FSDataOutputStream out;
	private static FSDataInputStream in ;
	private static BufferedReader br ;
	
	public static double distance(String point1, String point2) {
		p1 = stringToDoubles(point1);
		p2 = stringToDoubles(point2);
		sum = 0.0;
		for (int i = 0; i < DIMENSION; i++) {
			sum += Math.pow(Math.abs(p1[i] - p2[i]), DIMENSION);
		}
		return Math.pow(sum, 1d / DIMENSION);
	}

	public static double[] stringToDoubles(String s) {
		doubleString = s.split(SPLIT);
		doubles = new double[DIMENSION];
		for (int i = 0; i < DIMENSION; i++) {
			doubles[i] = Double.parseDouble(doubleString[i]);
		}
		return doubles;
	}

	public static String doubleToString(double[] ds) {
		sb = new StringBuffer();
		for (double d : ds) {
			sb.append(d + ",");
		}
		sb.deleteCharAt(sb.length() - 1);
		return sb.toString();
	}
	
	public static String createPartR(int k){
		index = String.valueOf(k);
		l = index.length();
		fName = "/part-r-";
		for(int i = 0; i < 5 - l; i ++){
			fName += "0";
		}
		return fName + index;
	}
	
	public static void firstClusters(Configuration conf) throws IOException{
		dPointPath = conf.get("dataPointsPath");
		currClusters = conf.get("currentClusterPath");
		updatedClusters = conf.get("updatedClusterPath");
		KNum = Integer.parseInt(conf.get("KNum"));
		center = new double[DIMENSION];
		Arrays.fill(center, 0.0);
		centerString = doubleToString(center);
		fsCurClus = FileSystem.get(URI.create(currClusters), conf);
		pathCurClus = new Path(currClusters);
		
		if(fsCurClus.exists(pathCurClus)){
			fsCurClus.delete(pathCurClus, true);
		}
		for(int i = 0; i <KNum; i ++){
			uri = currClusters + createPartR(i);
			out = fsCurClus.create(new Path(uri));
			out.write(centerString.getBytes());
			out.close();
		}
		
		fsPoint = FileSystem.get(URI.create(dPointPath), conf);
		fsUpdated = FileSystem.get(URI.create(updatedClusters), conf);
		pathUpdate = new Path(updatedClusters);
		if(fsUpdated.exists(pathUpdate)){
			fsUpdated.delete(pathUpdate, true);
		}
		in = fsPoint.open(new Path(dPointPath));
		br = new BufferedReader(new InputStreamReader(in));
		k = 0;
		while((line = br.readLine()) != null && (k < KNum)){
			uri = updatedClusters + createPartR(k);
			out = fsUpdated.create(new Path(uri));
			out.write(line.getBytes());
			out.close();
			k ++;
		}
	}

	public static boolean hasConverged(Configuration conf) throws IOException {
		currClusters = conf.get("currentClusterPath");
		updatedClusters = conf.get("updatedClusterPath");
		KNum = Integer.parseInt(conf.get("KNum"));
		currCenters = getCenters(currClusters, KNum, conf);
		updatedCenters = getCenters(updatedClusters, KNum, conf);		
		for (int i = 0; i < KNum; i++) {
			dist = distance(currCenters[i], updatedCenters[i]);
			if (dist > convergedPoint) {
				return false;
			}
		}
		return true;
	}
	
	public static String[] getCenters(String folderPath, int num, Configuration conf) throws IOException{
		String[] centers = new String[num];
		int count = 0;
		int index = 0;
		String line;
		while(count < num && index < num){
			String uri = folderPath + createPartR(index);
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			FSDataInputStream in = null;
			try{
				in = fs.open(new Path(uri));
				BufferedReader reader = new BufferedReader(new InputStreamReader(in));
				while((line = reader.readLine()) != null){
					centers[count] = line;
					count ++;
				}
				index ++;
			} finally{
				IOUtils.closeStream(in);
			}
		}
		return centers;
	}

	public static void updateCenters(Configuration conf) throws IOException {
		currClusters = conf.get("currentClusterPath");
		updatedClusters = conf.get("updatedClusterPath");
		KNum = Integer.parseInt(conf.get("KNum"));
		fsOut = FileSystem.get(URI.create(currClusters),conf);
		fsIn = FileSystem.get(URI.create(updatedClusters),conf);
		current = new Path(currClusters);
		if (fsOut.exists(current)) {
			fsOut.delete(current, true);
		}

		for(int i = 0; i < KNum; i ++){
			uriIn = updatedClusters + createPartR(i);
			uriOut = currClusters + createPartR(i);
			if(fsIn.exists(new Path(uriIn))){
				in = fsIn.open(new Path(uriIn));
				out = fsOut.create(new Path(uriOut));
				IOUtils.copyBytes(in, out, conf);
				in.close();
				out.close();
			}
		}
	}


}

