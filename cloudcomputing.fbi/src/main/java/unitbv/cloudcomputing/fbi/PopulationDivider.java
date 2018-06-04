package unitbv.cloudcomputing.fbi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PopulationDivider {

	public static class DivideMapper extends Mapper<Object, Text, Text, IntWritable> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] fields = value.toString().split(",");

			int roberies = Integer.parseInt(fields[2]);
			int population = Integer.parseInt(fields[1]);

			if (population < 10000) {
				context.write(new Text("10000"), new IntWritable(roberies));
			}
			if (population >= 10000 && roberies < 25000) {
				context.write(new Text("25000"), new IntWritable(roberies));
			}
			if (population >= 25000) {
				context.write(new Text("50000000"), new IntWritable(roberies));
			}
		}
	}

	public static class DivideReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private Map<Integer, Integer> data = new TreeMap<>();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}

			data.put(Integer.parseInt(key.toString()), sum);
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			Entry<Integer, Integer> previousEntry = data.entrySet().iterator().next();
			Boolean isGood = true;

			for (Entry<Integer, Integer> entry : data.entrySet()) {
				context.write(new Text(entry.getKey().toString()), new IntWritable(entry.getValue()));
				if (previousEntry.getValue() >= entry.getValue()) {
					isGood = false;
				}
				previousEntry = entry;
			}

			if (isGood) {
				context.write(new Text("Discrepancies:"), new IntWritable(1));
			} else {
				context.write(new Text("Discrepancies:"), new IntWritable(0));
			}

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJobName("PopulationDivider");

		job.setJarByClass(PopulationDivider.class);
		job.setMapperClass(DivideMapper.class);
		job.setReducerClass(DivideReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
