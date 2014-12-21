package KMeansMR;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class KMeansMR {
	public static class KMeansParallelMapper extends
			Mapper<Object, Text, IntWritable, Text> {

		private static String[] currCenters;
		private static int num;
		private static IntWritable keyOut = new IntWritable();
		private static Text valueOut = new Text();
		private static String point;
		private static double min, dist;

		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			String folder = conf.get("currentClusterPath");
			num = Integer.parseInt(conf.get("KNum"));
			currCenters = Utils.getCenters(folder, num, conf);
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			point = value.toString();
			min = Double.MAX_VALUE;
			for (int i = 0; i < num; i++) {
				dist = Utils.distance(currCenters[i], point);
				if (dist < min) {
					min = dist;
					keyOut.set(i);
				}
			}
			valueOut.set(point);
			context.write(keyOut, valueOut);
		}
	}

	public static class KMeansParallelReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
		private NullWritable outKey = NullWritable.get();
		private static double[] sum, cPoint, updatedCenter;
		double count;
		
		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			count = 0.0;
			sum = new double[Utils.DIMENSION];
			cPoint = new double[Utils.DIMENSION];
			updatedCenter = new double[Utils.DIMENSION];
			
			for (Text val : values) {
				cPoint = Utils.stringToDoubles(val.toString());
				for (int i = 0; i < Utils.DIMENSION; i++) {
					sum[i] += cPoint[i];
				}
				count++;
			}
			for (int i = 0; i < Utils.DIMENSION; i++) {
				updatedCenter[i] = sum[i] / count;
			}
			String outString = Utils.doublesToString(updatedCenter);
			context.write(outKey, new Text(outString));
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		int code = 1;
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: KMeansParallel <in> <out> <k>");
			System.exit(3);
		}		
		conf.set("dataPointsPath", args[0]);
		conf.set("currentClusterPath", args[1] + "/currCluster");
		conf.set("updatedClusterPath", args[1] + "/updatedClusters");
		conf.set("KNum", args[2]);
		int numReduceTasks = Integer.parseInt(args[2]);
		
		Utils.firstClusters(conf);
		while (!Utils.hasConverged(conf)) {
			Utils.updateCenters(conf);
			String outUri = args[1] + "/updatedClusters";
			FileSystem fs = FileSystem.get(URI.create(outUri), conf);
			Path out = new Path(outUri);
			if (fs.exists(out)) {
				fs.delete(out, true);
			}
			
			Job job = new Job(conf, "KMeansParallel");
			job.setJarByClass(KMeansMR.class);
			job.setMapperClass(KMeansParallelMapper.class);
			job.setReducerClass(KMeansParallelReducer.class);
			job.setNumReduceTasks(numReduceTasks);
			
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
		
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + "/updatedClusters"));
			code = job.waitForCompletion(true) ? 0 : 1;
		   }
		   System.exit(code);
		  }

}

