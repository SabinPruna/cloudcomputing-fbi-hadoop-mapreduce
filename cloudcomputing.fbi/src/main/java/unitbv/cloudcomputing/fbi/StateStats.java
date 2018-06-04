package unitbv.cloudcomputing.fbi;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class StateStats {

	public static class StatsMapper extends Mapper<Object, Text, Text, IntWritable> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] fields = value.toString().split(",");

			context.write(new Text("Population"), new IntWritable(Integer.parseInt(fields[1])));
			context.write(new Text("violent crime"), new IntWritable(Integer.parseInt(fields[2])));
			context.write(new Text("Murder and nonnegligent manslaughter"),
					new IntWritable(Integer.parseInt(fields[3])));
			context.write(new Text("Rape"), new IntWritable(Integer.parseInt(fields[4])));
			context.write(new Text("Robbery"), new IntWritable(Integer.parseInt(fields[5])));
			context.write(new Text("Aggravated assault"), new IntWritable(Integer.parseInt(fields[6])));
			context.write(new Text("Property crime"), new IntWritable(Integer.parseInt(fields[7])));
			context.write(new Text("Burglary"), new IntWritable(Integer.parseInt(fields[8])));
			context.write(new Text("Larcenytheft"), new IntWritable(Integer.parseInt(fields[9])));
			context.write(new Text("Motor vehicle theft"), new IntWritable(Integer.parseInt(fields[10])));
		}
	}

	public static class StatsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJobName("StateStats");

		job.setJarByClass(StateStats.class);
		job.setMapperClass(StatsMapper.class);
		job.setReducerClass(StatsReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
