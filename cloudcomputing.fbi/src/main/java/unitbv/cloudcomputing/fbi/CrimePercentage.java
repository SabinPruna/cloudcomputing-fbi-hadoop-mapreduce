package unitbv.cloudcomputing.fbi;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CrimePercentage {

	public static class PercentageMapper extends Mapper<Object, Text, Text, IntWritable> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] fields = value.toString().split(",");
			
			context.write(new Text("Murder and nonnegligent manslaughter"),
					new IntWritable(Integer.parseInt(fields[1])));
			context.write(new Text("Rape"), new IntWritable(Integer.parseInt(fields[2])));
			context.write(new Text("Robbery"), new IntWritable(Integer.parseInt(fields[3])));
			context.write(new Text("Aggravated assault"), new IntWritable(Integer.parseInt(fields[4])));
			context.write(new Text("Burglary"), new IntWritable(Integer.parseInt(fields[5])));
			context.write(new Text("Larcenytheft"), new IntWritable(Integer.parseInt(fields[6])));
			context.write(new Text("Motor vehicle theft"), new IntWritable(Integer.parseInt(fields[7])));
		}
	}

	public static class PercentageReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		private Map<Text, IntWritable> statePercentages = new HashMap<>();
		
		private int totalCrimes = 0;

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			totalCrimes += sum;

			statePercentages.put(new Text(key), new IntWritable(sum));
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Entry<Text, IntWritable> entry : statePercentages.entrySet()) {
				
				double valuePerColumn = Double.parseDouble(entry.getValue().toString());
				double percentAsDouble = (valuePerColumn / totalCrimes) * 100;
				Integer percent = (int) percentAsDouble;  
				
				context.write(new Text(entry.getKey()), new IntWritable(percent));
			}
		}
	}
	
	

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJobName("CrimePercentage");

		job.setJarByClass(CrimePercentage.class);
		job.setMapperClass(PercentageMapper.class);
		job.setReducerClass(PercentageReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
