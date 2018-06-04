package unitbv.cloudcomputing.fbi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import unitbv.cloudcomputing.fb.utils.FbiUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TopTenCleanupSort {

	public static class TopTenMapper extends Mapper<Object, Text, Text, IntWritable> {

		private Map<String, Integer> cityRecordMap = new HashMap<>();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] fields = value.toString().split(",");
			int violentCrimes = Integer.parseInt(fields[1]);

			cityRecordMap.put(fields[0], violentCrimes);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

			for (String key : cityRecordMap.keySet()) {
				context.write(new Text(key), new IntWritable(cityRecordMap.get(key)));
			}
		}
	}

	public static class TopTenReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private Map<Text, IntWritable> cityRecordsMap = new HashMap<>();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			cityRecordsMap.put(new Text(key), new IntWritable(values.iterator().next().get()));
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

			Map<Text, IntWritable> sortedMap = FbiUtils.sortByValues(cityRecordsMap);

			int counter = 0;
			for (Text key : sortedMap.keySet()) {
				if (counter++ == 10) {
					break;
				}
				context.write(key, sortedMap.get(key));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJobName("TopTenCleanupSort");

		job.setJarByClass(TopTenCleanupSort.class);
		job.setMapperClass(TopTenMapper.class);
		job.setReducerClass(TopTenReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}