package unitbv.cloudcomputing.fbi;

import java.io.IOException;
import java.util.TreeMap;
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

public class TopTenMostViolentCitiesReverse {

	public static class MostViolentCityMapper extends Mapper<Object, Text, IntWritable, Text> {
		private TreeMap<IntWritable, Text> violenceData = new TreeMap<IntWritable, Text>();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] fields = value.toString().split(",");
			int violentCrimes = Integer.parseInt(fields[1]);

			violenceData.put(new IntWritable(violentCrimes), new Text(fields[0]));

			if (violenceData.size() > 10) {
				violenceData.remove(violenceData.firstKey());
			}
		}
	}

	public static class MostViolentCityReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
		private TreeMap<IntWritable, Text> violenceData = new TreeMap<IntWritable, Text>();

		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text value : values) {
				violenceData.put(key, value);

				if (violenceData.size() > 10) {
					violenceData.remove(violenceData.firstKey());
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Entry<IntWritable, Text> entry : violenceData.descendingMap().entrySet()) {
				context.write(entry.getValue(), entry.getKey());
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "TopTenMostViolentCities");

		job.setJarByClass(TopTenMostViolentCities.class);
		job.setMapperClass(MostViolentCityMapper.class);
		job.setCombinerClass(MostViolentCityReducer.class);
		job.setReducerClass(MostViolentCityReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
