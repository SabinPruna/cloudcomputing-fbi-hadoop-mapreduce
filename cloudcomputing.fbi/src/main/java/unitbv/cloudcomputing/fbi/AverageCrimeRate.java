package unitbv.cloudcomputing.fbi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageCrimeRate {


	public static class AverageMapper extends Mapper<Object, Text, Text, IntWritable> {

		private int totalCrimes = 0;
		private Text totalCrimesCountText = new Text("totalCrimesCount");

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] fields = value.toString().split(",");

			int crimeRate = Integer.parseInt(fields[1]) + Integer.parseInt(fields[2]);
					
			totalCrimes += crimeRate;		
			
			context.write(new Text(fields[0]), new IntWritable(crimeRate));
			context.write(totalCrimesCountText, new IntWritable(1));
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
				context.write(new Text("totalCrimes"), new IntWritable(totalCrimes));
		}
	}

	public static class AverageReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private int average = 0;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
	        Configuration conf = context.getConfiguration();
	        int numberOfRecords = 0;

	        ArrayList<String> counts = new ArrayList<String>(Arrays.asList(conf.get("totalCrimesCount").split(",")));
	        counts.remove(0);
	        
	        numberOfRecords = counts.size();	
	        Integer average =  Integer.parseInt(conf.get("totalCrimes").split(",")[1]) / numberOfRecords;
	    }	
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
			if(values.iterator().next().get() > average) {
				context.write(key, new IntWritable(values.iterator().next().get()));
			}
			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJobName("AverageCrimeRate");

		job.setJarByClass(AverageCrimeRate.class);
		job.setMapperClass(AverageMapper.class);
		job.setReducerClass(AverageReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
